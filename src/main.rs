use std::{
    fs::File,
    io::{Error, ErrorKind, Read, Result, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::sleep,
    time::{Duration, Instant},
};

use clap::Parser;
use zip::ZipArchive;

type FixedString = Box<str>;
type FixedVec<T> = Box<[T]>;

/// Read the file's content into a list of its lines
fn read_file_to_lines(file: &mut impl Read) -> Result<FixedVec<FixedString>> {
    let mut file_content = String::new();
    file.read_to_string(&mut file_content)?;
    Ok(file_content.lines().map(|line| line.into()).collect())
}

/// Read ACMI file data into a list of lines.
fn read_acmi_file(filepath: &Path) -> Result<FixedVec<FixedString>> {
    let name = filepath
        .file_name()
        .ok_or(Error::other("No File Name!"))?
        .to_string_lossy();

    if name.ends_with(".zip.acmi") {
        let file = File::open(filepath)?;
        let mut archive = ZipArchive::new(file)?;
        let file_name = archive
            .file_names()
            .filter(|name| name.ends_with(".acmi") || name.ends_with(".txt"))
            .map(|name| name.to_owned())
            .next();

        if let Some(file_name) = file_name {
            let mut file = archive.by_name(&file_name)?;
            read_file_to_lines(&mut file)
        } else {
            Err(Error::other("Couldn't find .acmi or .txt file in archive!"))
        }
    } else if name.ends_with(".acmi") || name.ends_with(".txt") {
        let mut file = File::open(filepath)?;
        read_file_to_lines(&mut file)
    } else {
        Err(Error::other(format!("Unsupported file format: {name}!")))
    }
}

/// Write all the bytes from the buffer into the destination, return if continue_loops is false
fn write_all(buf: &[u8], dest: &mut impl Write, continue_loops: &Arc<AtomicBool>) -> Result<()> {
    let mut bytes_written = 0;

    let blocking_duration = Duration::from_millis(10);

    while bytes_written < buf.len() {
        if !continue_loops.load(Ordering::Acquire) {
            return Err(ErrorKind::Interrupted.into());
        }
        match dest.write(&buf[bytes_written..]) {
            Ok(0) => return Err(ErrorKind::UnexpectedEof.into()),
            Ok(n) => bytes_written += n,
            Err(err) if err.kind() == ErrorKind::WouldBlock => sleep(blocking_duration),
            Err(err) => return Err(err),
        }
    }

    Ok(())
}

/// Read from the Read into the buffer, break if continue_loops is false
fn read(buf: &mut [u8], source: &mut impl Read, continue_loops: &Arc<AtomicBool>) -> Result<usize> {
    let blocking_duration = Duration::from_millis(10);

    loop {
        if !continue_loops.load(Ordering::Acquire) {
            return Err(ErrorKind::Interrupted.into());
        }
        match source.read(buf) {
            Ok(n) => return Ok(n),
            Err(err) if err.kind() == ErrorKind::WouldBlock => sleep(blocking_duration),
            Err(err) => return Err(err),
        }
    }
}

/// Create and configure the server socket.
fn create_server_socket(host: &str, port: u16) -> Result<TcpListener> {
    let listener = TcpListener::bind((host, port))?;
    listener.set_nonblocking(true)?;

    if let Ok(addr) = listener.local_addr() {
        println!("Server listening on {addr}");
    }

    Ok(listener)
}

/// Perform the Tacview handshake with the client.
fn perform_handshake(
    stream: &mut TcpStream,
    host_username: &str,
    continue_loops: &Arc<AtomicBool>,
) -> Result<()> {
    let timeout = Some(Duration::from_secs(5));
    stream.set_read_timeout(timeout)?;
    stream.set_write_timeout(timeout)?;

    let handshake =
        format!("XtraLib.Stream.0\nTacview.RealTimeTelemetry.0\nHost {host_username}\n\0");
    write_all(handshake.as_bytes(), stream, continue_loops)?;
    println!("Sent");

    // Wait for client to send handshake
    let sleep_dur = Duration::from_millis(100);
    sleep(sleep_dur);
    println!("Slept");

    // Read the client handshake
    let mut received = String::new();
    let mut last_read = 1024;
    while last_read == 1024 {
        let mut buf = [0; 1024];
        last_read = read(&mut buf, stream, continue_loops)?;
        let next_str = str::from_utf8(&buf[..last_read]).map_err(|err| {
            Error::other(format!("Error while converting handshake to UTF-8: {err}"))
        });
        received.push_str(next_str?);
        println!("read");
    }

    // Check if the client handshake is valid
    println!("Client handshake: {received}");

    if received.is_empty() {
        Err(Error::other("Bad Handshake"))
    } else {
        Ok(())
    }
}

/// Find the first timestamp in the ACMI data
fn find_first_timestamp(acmi_data: &[FixedString]) -> f32 {
    acmi_data
        .iter()
        .filter_map(|line| line.strip_prefix('#').and_then(|line| line.parse().ok()))
        .next()
        .unwrap_or_default()
}

fn send_block(
    stream: &mut TcpStream,
    lines: &[FixedString],
    continue_loops: &Arc<AtomicBool>,
) -> Result<()> {
    for line in lines {
        write_all(line.as_bytes(), stream, continue_loops).map_err(|err| {
            Error::new(
                err.kind(),
                "Transmission stopped because the connection was closed.",
            )
        })?;

        stream.write_all(b"\n")?;
    }

    Ok(())
}

/// Sleep the correct amount of time before sending a block. Doesn't sleep if we're seeking
/// currently.
fn sleep_before_send(
    last_to_current_delta: f32,
    actual_wait_start: Instant,
    seeking: &bool,
    time_multiplier: &f32,
) {
    // Only sleep if we're not seeking and it's necessary
    if !seeking && last_to_current_delta > 0. {
        let sleep_secs = last_to_current_delta / time_multiplier;
        let already_slept = Instant::now() - actual_wait_start;
        let sleep_dur = Duration::from_secs_f64(sleep_secs as f64).saturating_sub(already_slept);
        sleep(sleep_dur);
    }
}

/// Stream ACMI data to the connected client.
fn stream_acmi_data(
    stream: &mut TcpStream,
    acmi_data: &[FixedString],
    time_multiplier: f32,
    start_time: f32,
    continue_loops: &Arc<AtomicBool>,
) -> Result<()> {
    // Find the first timestamp in the file to use as baseline
    let first_timestamp = find_first_timestamp(acmi_data);
    // We strategically use this as the start time for the first two blocks.
    // This way we don't wait ridiculous amounts
    let mut current_block = first_timestamp;

    let mut start_of_block = 0;
    let mut last_to_current_delta = 0f32;
    let mut actual_wait_start = Instant::now();

    let target_start_time = first_timestamp + start_time;
    let mut seeking = start_time > 0.;

    if seeking {
        println!(
            "File starts at {first_timestamp:.2}s, seeking to \
            {target_start_time:.2}s (offset +{start_time:.2}s)"
        );
    }

    for (idx, line) in acmi_data.iter().enumerate() {
        if !continue_loops.load(Ordering::Acquire) {
            eprintln!("Breaking out of stream because of Ctrl-C");
            break;
        }

        if let Some(line) = line.strip_prefix('#')
            && let Ok(next_block) = line.parse::<f32>()
        {
            sleep_before_send(
                last_to_current_delta,
                actual_wait_start,
                &seeking,
                &time_multiplier,
            );

            send_block(stream, &acmi_data[start_of_block..idx], continue_loops)?;
            actual_wait_start = Instant::now();

            // If we have reached the desired start point, the next block can be sent after waiting
            if seeking && current_block >= target_start_time {
                seeking = false;
                println!("Started streaming from time {current_block:.2}s")
            }

            let current_to_next_delta = next_block - current_block;

            // Switch around for upcoming block

            current_block = next_block;
            last_to_current_delta = current_to_next_delta;
            start_of_block = idx;
        }
    }

    sleep_before_send(
        last_to_current_delta,
        actual_wait_start,
        &seeking,
        &time_multiplier,
    );

    send_block(stream, &acmi_data[start_of_block..], continue_loops)?;

    Ok(())
}

/// Main server loop that accepts connections and streams ACMI data.
fn run_server(
    filepath: &Path,
    time_multiplier: f32,
    host: &str,
    port: u16,
    start_time: f32,
    continue_loops: &Arc<AtomicBool>,
) -> Result<()> {
    let acmi_data = read_acmi_file(filepath)?;

    let mut host_username = "Tacview Realtime Recorder".to_owned();

    if let Some(filename) = filepath.file_name().and_then(|name| name.to_str()) {
        host_username += ": ";
        host_username += filename;
    }

    let server_socket = create_server_socket(host, port)?;

    let blocking_duration = Duration::from_millis(10);

    while continue_loops.load(Ordering::Acquire) {
        let stream = server_socket.accept();
        match stream {
            Ok((mut stream, addr)) => {
                println!("Client connected from {}", addr);

                let handshake_res = perform_handshake(&mut stream, &host_username, continue_loops);
                if let Err(err) = handshake_res {
                    eprintln!("Handshake failed: {err}");
                    continue;
                }

                println!("Streaming ACM data...");
                let stream_res = stream_acmi_data(
                    &mut stream,
                    &acmi_data,
                    time_multiplier,
                    start_time,
                    continue_loops,
                );
                if let Err(err) = stream_res {
                    eprintln!("Stream ended early: {err}");
                } else {
                    println!("Stream complete");
                }
            }
            Err(err) if err.kind() == ErrorKind::WouldBlock => sleep(blocking_duration),
            Err(err) => eprintln!("Listener was closed: {err}"),
        }
    }
    eprintln!("Shutting down server");

    Ok(())
}

/// Main entry point with command line argument parsing.
fn main() -> Result<()> {
    let args = Args::parse();

    let continue_loops = Arc::new(AtomicBool::new(true));

    let r = continue_loops.clone();

    let _ = ctrlc::set_handler(move || r.store(false, Ordering::Release));

    run_server(
        &args.filename,
        args.time_multiplier,
        &args.host,
        args.port,
        args.start_time,
        &continue_loops,
    )?;

    Ok(())
}

/// Stream ACMI file data via Tacview Real Time Telemetry
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Path to the ACMI file (.acmi, .txt, or .zip.acmi)
    filename: PathBuf,
    /// Time multiplier for playback speed
    #[arg(short, long = "timemultiplier", default_value_t = 32.)]
    time_multiplier: f32,
    /// Start time offset in seconds from the beginning of the file
    #[arg(short, long = "start-time", default_value_t = 0.)]
    start_time: f32,
    /// Host to bind to
    #[arg(long, default_value_t = "localhost".to_owned())]
    host: String,
    /// Port to bind to
    #[arg(short, long, default_value_t = 42674)]
    port: u16,
}

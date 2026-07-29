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
    time::Duration,
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
fn perform_handshake(stream: &mut TcpStream, host_username: &str) -> Result<()> {
    let timeout = Some(Duration::from_secs(5));
    stream.set_read_timeout(timeout)?;
    stream.set_write_timeout(timeout)?;

    let handshake =
        format!("XtraLib.Stream.0\nTacview.RealTimeTelemetry.0\nHost {host_username}\n\0");
    stream.write_all(handshake.as_bytes())?;

    // Wait for client to send handshake
    let sleep_dur = Duration::from_millis(100);
    sleep(sleep_dur);

    // Read the client handshake
    let mut received = String::new();
    let mut last_read = 1024;
    while last_read == 1024 {
        let mut buf = [0; 1024];
        last_read = stream.read(&mut buf)?;
        let next_str = str::from_utf8(&buf[..last_read]).map_err(|err| {
            Error::other(format!("Error while converting handshake to UTF-8: {err}"))
        });
        received.push_str(next_str?);
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
fn find_first_timestamp<'a>(acmi_data: impl IntoIterator<Item = &'a FixedString>) -> f32 {
    acmi_data
        .into_iter()
        .filter_map(|line| line.strip_prefix('#').and_then(|line| line.parse().ok()))
        .next()
        .unwrap_or_default()
}

fn send_buf(stream: &mut TcpStream, buffer: &String) -> Result<()> {
    stream.write_all(buffer.as_bytes()).map_err(|err| {
        Error::new(
            err.kind(),
            "Transmission stopped because the connection was closed.",
        )
    })?;

    Ok(())
}

/// Stream ACMI data to the connected client.
fn stream_acmi_data<'a, T>(
    stream: &mut TcpStream,
    acmi_data: T,
    time_multiplier: f32,
    start_time: f32,
    continue_loops: &Arc<AtomicBool>,
) -> Result<()>
where
    T: IntoIterator<Item = &'a FixedString>,
    T::IntoIter: Clone,
{
    let acmi_data = acmi_data.into_iter();

    let mut buffer = String::new();
    let mut buf_start_time = 0f32;

    // Find the first timestamp in the file to use as baseline
    let first_timestamp = find_first_timestamp(acmi_data.clone());
    let target_start_time = first_timestamp + start_time;
    let mut seeking = start_time > 0.;

    if seeking {
        println!(
            "File starts at {first_timestamp:.2}s, seeking to \
            {target_start_time:.2}s (offset +{start_time:.2}s)"
        );
    }

    for line in acmi_data {
        if !continue_loops.load(Ordering::Acquire) {
            eprintln!("Breaking out of stream because of Ctrl-C");
            break;
        }

        if let Some(line) = line.strip_prefix('#')
            && let Ok(next_buf_time) = line.parse::<f32>()
        {
            send_buf(stream, &buffer)?;

            if seeking && buf_start_time >= target_start_time {
                seeking = false;
                println!("Started streaming from time {buf_start_time:.2}s")
            }

            // Only sleep if we're not seeking
            if buf_start_time > 0. && !seeking {
                let sleep_secs = (next_buf_time - buf_start_time) / time_multiplier;
                let sleep_dur = Duration::from_secs_f64(sleep_secs as f64);
                sleep(sleep_dur);
            }

            buffer.clear();
            buf_start_time = next_buf_time;
        }

        buffer.push_str(line);
        buffer.push('\n');
    }

    if !buffer.is_empty() {
        send_buf(stream, &buffer)?;
    }

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

    let blocking_duration = Duration::from_millis(100);

    while continue_loops.load(Ordering::Acquire) {
        let stream = server_socket.accept();
        match stream {
            Ok((mut stream, addr)) => {
                stream.set_nonblocking(false)?;
                println!("Client connected from {}", addr);

                let handshake_res = perform_handshake(&mut stream, &host_username);
                if let Err(err) = handshake_res {
                    eprintln!("Handshake failed: {err}, {}", err.kind());
                    continue;
                }

                println!("Streaming ACM data...");
                let stream_res = stream_acmi_data(
                    &mut stream,
                    acmi_data.iter(),
                    time_multiplier,
                    start_time,
                    continue_loops,
                );
                if let Err(err) = stream_res {
                    eprintln!("Stream ended early: {err}, {}", err.kind());
                } else {
                    println!("Stream complete");
                }
            }
            Err(err) if err.kind() == ErrorKind::WouldBlock => sleep(blocking_duration),
            Err(err) => eprintln!("Listener was closed: {err}, {}", err.kind()),
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

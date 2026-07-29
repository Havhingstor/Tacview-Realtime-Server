use std::{
    fs::File,
    io::{Error, Read, Result, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
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
        let mut file = archive.by_name("acmi.txt")?;
        read_file_to_lines(&mut file)
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
    // TODO: Do we need `listen` or `timeout`?
    if let Ok(addr) = listener.local_addr() {
        println!("Server listening on {addr}");
    }

    Ok(listener)
}

/// Perform the Tacview handshake with the client.
fn perform_handshake(stream: &mut TcpStream) -> Result<()> {
    let timeout = Some(Duration::from_secs(5));
    stream.set_read_timeout(timeout)?;
    stream.set_write_timeout(timeout)?;

    let handshake = b"XtraLib.Stream.0\nTacview.RealTimeTelemetry.0\nHost streamtest\n\0";
    stream.write_all(handshake)?;

    // Wait for client to send handshake
    let sleep_dur = Duration::from_millis(100);
    sleep(sleep_dur);

    // Read the client handshake
    let mut received = String::new();
    stream.read_to_string(&mut received)?;

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

/// Stream ACMI data to the connected client.
fn stream_acmi_data<'a, T>(
    stream: &mut TcpStream,
    acmi_data: T,
    time_multiplier: f32,
    start_time: f32,
) -> Result<()>
where
    T: IntoIterator<Item = &'a FixedString>,
    T::IntoIter: Clone,
{
    let acmi_data = acmi_data.into_iter();

    let mut buffer = String::new();
    let mut last_buffer = String::new();
    let mut last_buffer_time = 0f32;

    // Find the first timestamp in the file to use as baseline
    let first_timestamp = find_first_timestamp(acmi_data.clone());
    let target_start_time = first_timestamp + start_time;
    let mut seeking = start_time > 0.;
    let mut first_frame_after_seek = true;

    if seeking {
        println!(
            "File starts at {first_timestamp:.2}s, seeking to \
            {target_start_time:.2}s (offset +{start_time:.2}s)"
        );
    }

    for line in acmi_data {
        buffer.push_str(line);

        if let Some(line) = line.strip_prefix('#')
            && let Ok(cur_time) = line.parse::<f32>()
        {
            if seeking && cur_time >= target_start_time {
                seeking = false;
                first_frame_after_seek = true;
                println!("Started streaming from time {cur_time:.2}s")
            }

            stream.write_all(last_buffer.as_bytes())?;

            // Only sleep if we're not seeking and not the first frame after seek
            if last_buffer_time > 0. && !first_frame_after_seek && !seeking {
                let sleep_secs = (cur_time - last_buffer_time) / time_multiplier;
                let sleep_dur = Duration::from_secs_f64(sleep_secs as f64);
                sleep(sleep_dur);
            }

            last_buffer = buffer;
            last_buffer_time = cur_time;
            buffer = String::new();
            first_frame_after_seek = false;
        }
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
) -> Result<()> {
    let acmi_data = read_acmi_file(filepath)?;

    let server_socket = create_server_socket(host, port)?;

    for stream in server_socket.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("Client connected from {}", stream.peer_addr()?);

                let handshake_res = perform_handshake(&mut stream);
                if let Err(err) = handshake_res {
                    eprintln!("Handshake failed, closing connection: {err}");
                    continue;
                }

                println!("Streaming ACM data...");
                let stream_res =
                    stream_acmi_data(&mut stream, acmi_data.iter(), time_multiplier, start_time);
                if let Err(err) = stream_res {
                    eprintln!("Streaming failed, closing connection: {err}");
                } else {
                    println!("Stream complete");
                }
            }
            Err(err) => println!("Encountered error: {err}"),
        }
    }

    Ok(())
}

/// Main entry point with command line argument parsing.
fn main() {
    let args = Args::parse();
    let run_res = run_server(
        &args.filename,
        args.time_multiplier,
        &args.host,
        args.port,
        args.start_time,
    );

    if let Err(err) = run_res {
        eprintln!("Couldn't run server: {err}");
    }
}

/// Stream ACMI file data via Tacview Real Time Telemetry
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Path to the ACMI file (.acmi, .txt, or .zip.acmi)
    filename: PathBuf,
    /// Time multiplier for playback speed (default: 32)
    #[arg(short, long = "timemultiplier", default_value_t = 32.)]
    time_multiplier: f32,
    /// Start time offset in seconds from the beginning of the file (default: 0)
    #[arg(short, long = "start-time", default_value_t = 0.)]
    start_time: f32,
    /// Host to bind to (default: localhost)
    #[arg(long, default_value_t = "localhost".to_string())]
    host: String,
    /// Port to bind to (default: 42674)
    #[arg(short, long, default_value_t = 42674)]
    port: u16,
}

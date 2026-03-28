use crate::types::{CueFile, CueSheet, CueTime, CueTrack};

/// Parse a CUE sheet from text content.
pub fn parse_cue_sheet(content: &str) -> Result<CueSheet, String> {
    let mut sheet = CueSheet {
        performer: None,
        title: None,
        genre: None,
        date: None,
        catalog: None,
        files: Vec::new(),
    };

    let mut current_file: Option<CueFile> = None;
    let mut current_track: Option<CueTrack> = None;
    let mut saw_index01 = false;

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let cmd = line.split_whitespace().next().unwrap_or("");
        match cmd.to_ascii_uppercase().as_str() {
            "REM" => parse_rem(line, &mut sheet),
            "CATALOG" => sheet.catalog = extract_value(line, "CATALOG"),
            "PERFORMER" => {
                let val = extract_value(line, "PERFORMER");
                if let Some(ref mut track) = current_track {
                    track.performer = val;
                } else {
                    sheet.performer = val;
                }
            }
            "TITLE" => {
                let val = extract_value(line, "TITLE");
                if let Some(ref mut track) = current_track {
                    track.title = val;
                } else {
                    sheet.title = val;
                }
            }
            "FILE" => {
                flush_track(&mut current_track, &mut current_file, saw_index01)?;
                // Flush current file into sheet
                if let Some(file) = current_file.take() {
                    sheet.files.push(file);
                }
                let (filename, file_type) = parse_file_line(line);
                current_file = Some(CueFile {
                    filename,
                    file_type,
                    tracks: Vec::new(),
                });
            }
            "TRACK" => {
                flush_track(&mut current_track, &mut current_file, saw_index01)?;
                let (number, track_type) = parse_track_line(line)?;
                current_track = Some(CueTrack {
                    number,
                    track_type,
                    title: None,
                    performer: None,
                    isrc: None,
                    pregap: None,
                    index00: None,
                    index01: CueTime::ZERO,
                    postgap: None,
                });
                saw_index01 = false;
            }
            "INDEX" => {
                if let Some(ref mut track) = current_track
                    && parse_index(line, track)?
                {
                    saw_index01 = true;
                }
            }
            "PREGAP" => {
                if let Some(ref mut track) = current_track {
                    track.pregap = parse_time_arg(line, "PREGAP");
                }
            }
            "POSTGAP" => {
                if let Some(ref mut track) = current_track {
                    track.postgap = parse_time_arg(line, "POSTGAP");
                }
            }
            "ISRC" => {
                if let Some(ref mut track) = current_track {
                    track.isrc = extract_value(line, "ISRC");
                }
            }
            _ => {} // ignore unknown commands
        }
    }

    flush_track(&mut current_track, &mut current_file, saw_index01)?;
    if let Some(file) = current_file {
        sheet.files.push(file);
    }

    Ok(sheet)
}

/// Validate the current track (AUDIO tracks must have INDEX 01) and push
/// it into the current file. Takes ownership via Option::take().
fn flush_track(
    current_track: &mut Option<CueTrack>,
    current_file: &mut Option<CueFile>,
    saw_index01: bool,
) -> Result<(), String> {
    if let Some(track) = current_track.as_ref()
        && track.track_type == "AUDIO"
        && !saw_index01
    {
        return Err(format!("track {} missing INDEX 01", track.number));
    }
    if let Some(track) = current_track.take()
        && let Some(file) = current_file.as_mut()
    {
        file.tracks.push(track);
    }
    Ok(())
}

fn parse_rem(line: &str, sheet: &mut CueSheet) {
    let parts: Vec<&str> = line.splitn(3, ' ').collect();
    if parts.len() < 3 {
        return;
    }
    match parts[1].to_ascii_uppercase().as_str() {
        "GENRE" => sheet.genre = Some(unquote(parts[2])),
        "DATE" => sheet.date = Some(unquote(parts[2])),
        _ => {} // ignore unknown REM fields
    }
}

fn parse_file_line(line: &str) -> (String, String) {
    // FILE "filename" TYPE  or  FILE filename TYPE
    // "FILE" is always 4 ASCII bytes; use the keyword length for clarity
    let after_cmd = line["FILE".len()..].trim();
    if let Some(rest) = after_cmd.strip_prefix('"')
        && let Some(end_quote) = rest.find('"')
    {
        let filename = rest[..end_quote].to_string();
        let file_type = rest[end_quote + 1..].trim().to_string();
        return (filename, file_type);
    }
    // Unquoted: last word is file type
    let parts: Vec<&str> = after_cmd.rsplitn(2, ' ').collect();
    if parts.len() == 2 {
        (parts[1].to_string(), parts[0].to_string())
    } else {
        (after_cmd.to_string(), String::new())
    }
}

fn parse_track_line(line: &str) -> Result<(u32, String), String> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 3 {
        return Err(format!("invalid TRACK line: {line}"));
    }
    let number: u32 = parts[1]
        .parse()
        .map_err(|_| format!("invalid track number: {}", parts[1]))?;
    let track_type = parts[2].to_ascii_uppercase();
    Ok((number, track_type))
}

/// Returns true if INDEX 01 was set.
fn parse_index(line: &str, track: &mut CueTrack) -> Result<bool, String> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 3 {
        return Err(format!("invalid INDEX line: {line}"));
    }
    let index_num: u32 = parts[1]
        .parse()
        .map_err(|_| format!("invalid index number: {}", parts[1]))?;
    let time = parse_time(parts[2])?;
    let is_index01 = index_num == 1;
    match index_num {
        0 => track.index00 = Some(time),
        1 => track.index01 = time,
        _ => {} // INDEX 02+ ignored
    }
    Ok(is_index01)
}

fn parse_time(s: &str) -> Result<CueTime, String> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 3 {
        return Err(format!("invalid time format: {s}"));
    }
    let minutes: u32 = parts[0]
        .parse()
        .map_err(|_| format!("invalid minutes: {}", parts[0]))?;
    let seconds: u32 = parts[1]
        .parse()
        .map_err(|_| format!("invalid seconds: {}", parts[1]))?;
    let frames: u32 = parts[2]
        .parse()
        .map_err(|_| format!("invalid frames: {}", parts[2]))?;
    if seconds > 59 {
        return Err(format!("seconds out of range (0-59): {seconds}"));
    }
    if frames > 74 {
        return Err(format!("frames out of range (0-74): {frames}"));
    }
    Ok(CueTime::new(minutes, seconds, frames))
}

fn parse_time_arg(line: &str, cmd: &str) -> Option<CueTime> {
    let after = line[cmd.len()..].trim();
    parse_time(after).ok()
}

fn extract_value(line: &str, cmd: &str) -> Option<String> {
    let after = line[cmd.len()..].trim();
    if after.is_empty() {
        None
    } else {
        Some(unquote(after))
    }
}

fn unquote(s: &str) -> String {
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_CUE: &str = r#"REM GENRE Electronic
REM DATE 1998
PERFORMER "Various Artists"
TITLE "Rebirth of Cool, Volume 4"
FILE "album.flac" WAVE
  TRACK 01 AUDIO
    TITLE "Bug Powder Dust"
    PERFORMER "Kruder & Dorfmeister"
    INDEX 01 00:00:00
  TRACK 02 AUDIO
    TITLE "Innervisions"
    PERFORMER "DJ Cam"
    INDEX 01 04:35:12
  TRACK 03 AUDIO
    TITLE "Lebanese Blonde"
    PERFORMER "Thievery Corporation"
    INDEX 01 07:49:57
"#;

    #[test]
    fn test_parse_basic() {
        let sheet = parse_cue_sheet(SAMPLE_CUE).unwrap();
        assert_eq!(sheet.performer.as_deref(), Some("Various Artists"));
        assert_eq!(sheet.title.as_deref(), Some("Rebirth of Cool, Volume 4"));
        assert_eq!(sheet.genre.as_deref(), Some("Electronic"));
        assert_eq!(sheet.date.as_deref(), Some("1998"));
        assert_eq!(sheet.files.len(), 1);
        assert_eq!(sheet.files[0].filename, "album.flac");
        assert_eq!(sheet.files[0].file_type, "WAVE");
    }

    #[test]
    fn test_parse_tracks() {
        let sheet = parse_cue_sheet(SAMPLE_CUE).unwrap();
        let tracks: Vec<&CueTrack> = sheet.tracks().collect();
        assert_eq!(tracks.len(), 3);

        assert_eq!(tracks[0].number, 1);
        assert_eq!(tracks[0].title.as_deref(), Some("Bug Powder Dust"));
        assert_eq!(tracks[0].performer.as_deref(), Some("Kruder & Dorfmeister"));
        assert_eq!(tracks[0].index01, CueTime::new(0, 0, 0));

        assert_eq!(tracks[1].number, 2);
        assert_eq!(tracks[1].index01, CueTime::new(4, 35, 12));

        assert_eq!(tracks[2].number, 3);
        assert_eq!(tracks[2].index01, CueTime::new(7, 49, 57));
    }

    #[test]
    fn test_parse_catalog_and_isrc() {
        let cue = r#"CATALOG 0731454841726
FILE "test.wav" WAVE
  TRACK 01 AUDIO
    ISRC GBAYE0200145
    INDEX 01 00:00:00
"#;
        let sheet = parse_cue_sheet(cue).unwrap();
        assert_eq!(sheet.catalog.as_deref(), Some("0731454841726"));
        let tracks: Vec<&CueTrack> = sheet.tracks().collect();
        assert_eq!(tracks[0].isrc.as_deref(), Some("GBAYE0200145"));
    }

    #[test]
    fn test_parse_pregap() {
        let cue = r#"FILE "test.wav" WAVE
  TRACK 01 AUDIO
    PREGAP 00:02:00
    INDEX 01 00:00:00
"#;
        let sheet = parse_cue_sheet(cue).unwrap();
        let tracks: Vec<&CueTrack> = sheet.tracks().collect();
        assert_eq!(tracks[0].pregap, Some(CueTime::new(0, 2, 0)));
    }

    #[test]
    fn test_parse_index00() {
        let cue = r#"FILE "test.wav" WAVE
  TRACK 01 AUDIO
    INDEX 00 00:00:00
    INDEX 01 00:02:33
"#;
        let sheet = parse_cue_sheet(cue).unwrap();
        let tracks: Vec<&CueTrack> = sheet.tracks().collect();
        assert_eq!(tracks[0].index00, Some(CueTime::new(0, 0, 0)));
        assert_eq!(tracks[0].index01, CueTime::new(0, 2, 33));
    }

    #[test]
    fn test_track_count() {
        let sheet = parse_cue_sheet(SAMPLE_CUE).unwrap();
        assert_eq!(sheet.track_count(), 3);
    }

    #[test]
    fn test_case_insensitive() {
        let cue = "rem genre Jazz\nrem date 2005\nperformer \"Test\"\ntitle \"Album\"\nfile \"test.wav\" wave\n  track 01 audio\n    index 01 00:00:00\n";
        let sheet = parse_cue_sheet(cue).unwrap();
        assert_eq!(sheet.genre.as_deref(), Some("Jazz"));
        assert_eq!(sheet.date.as_deref(), Some("2005"));
        assert_eq!(sheet.track_count(), 1);
    }

    #[test]
    fn test_missing_index01_is_error() {
        let cue = r#"FILE "test.wav" WAVE
  TRACK 01 AUDIO
    TITLE "No index"
"#;
        assert!(parse_cue_sheet(cue).is_err());
    }

    #[test]
    fn test_invalid_seconds_rejected() {
        let cue = r#"FILE "test.wav" WAVE
  TRACK 01 AUDIO
    INDEX 01 00:60:00
"#;
        assert!(parse_cue_sheet(cue).is_err());
    }

    #[test]
    fn test_invalid_frames_rejected() {
        let cue = r#"FILE "test.wav" WAVE
  TRACK 01 AUDIO
    INDEX 01 00:00:75
"#;
        assert!(parse_cue_sheet(cue).is_err());
    }

    #[test]
    fn test_postgap() {
        let cue = r#"FILE "test.wav" WAVE
  TRACK 01 AUDIO
    INDEX 01 00:00:00
    POSTGAP 00:02:00
"#;
        let sheet = parse_cue_sheet(cue).unwrap();
        let tracks: Vec<&CueTrack> = sheet.tracks().collect();
        assert_eq!(tracks[0].postgap, Some(CueTime::new(0, 2, 0)));
    }

    #[test]
    fn test_unquoted_filename() {
        let cue = "FILE test.wav WAVE\n  TRACK 01 AUDIO\n    INDEX 01 00:00:00\n";
        let sheet = parse_cue_sheet(cue).unwrap();
        assert_eq!(sheet.files[0].filename, "test.wav");
        assert_eq!(sheet.files[0].file_type, "WAVE");
    }

    #[test]
    fn test_non_audio_track_no_index01_ok() {
        let cue = r#"FILE "data.bin" BINARY
  TRACK 01 MODE1/2352
FILE "audio.wav" WAVE
  TRACK 02 AUDIO
    INDEX 01 00:00:00
"#;
        let sheet = parse_cue_sheet(cue).unwrap();
        let all_tracks: Vec<&CueTrack> = sheet.tracks().collect();
        assert_eq!(all_tracks.len(), 2);
        assert_eq!(all_tracks[0].track_type, "MODE1/2352");
        assert_eq!(all_tracks[1].track_type, "AUDIO");
        assert_eq!(sheet.track_count(), 1); // only audio tracks
    }

    #[test]
    fn test_multi_file_cue() {
        let cue = r#"PERFORMER "Test"
TITLE "Split"
FILE "track01.wav" WAVE
  TRACK 01 AUDIO
    TITLE "First"
    INDEX 01 00:00:00
FILE "track02.wav" WAVE
  TRACK 02 AUDIO
    TITLE "Second"
    INDEX 01 00:00:00
"#;
        let sheet = parse_cue_sheet(cue).unwrap();
        assert_eq!(sheet.files.len(), 2);
        assert_eq!(sheet.track_count(), 2);

        // Absolute offsets with 3-minute files
        let (offsets, total) = sheet.absolute_offsets(&[13500, 13500]);
        assert_eq!(offsets, vec![0, 13500]);
        assert_eq!(total, 27000);
    }
}

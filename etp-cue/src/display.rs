use crate::types::{CueSheet, CueTime, CueTrack};

/// Format an album summary matching the mockup in docs/album-display-mockup.txt.
///
/// Durations are in MM:SS:FF (native CUE format). If `disc_id` is provided,
/// it's shown at the bottom. `file_durations` provides per-FILE sector counts
/// for computing track durations and absolute offsets.
pub fn format_album_summary(
    sheet: &CueSheet,
    file_durations: &[u64],
    disc_id: Option<&str>,
) -> String {
    let mut out = String::new();

    let year = sheet.date.as_deref().unwrap_or("????");
    let artist = sheet.performer.as_deref().unwrap_or("Unknown Artist");
    let title = sheet.title.as_deref().unwrap_or("Unknown Album");
    out.push_str(&format!("  [{year}] {artist}: {title}"));
    if let Some(genre) = &sheet.genre {
        out.push_str(&format!(" ({genre})"));
    }
    out.push_str("\n\n");

    let abs_tracks = absolute_audio_tracks(sheet, file_durations);
    let num_width = if abs_tracks.len() >= 10 { 2 } else { 1 };

    for (i, at) in abs_tracks.iter().enumerate() {
        let duration = abs_track_duration(at, abs_tracks.get(i + 1), file_durations);
        let dur_str = match duration {
            Some(d) => format!("({d})"),
            None => "(??:??:??)".into(),
        };

        let track_line = match (&at.track.performer, &sheet.performer) {
            (Some(tp), Some(ap)) if tp != ap => {
                let title = at.track.title.as_deref().unwrap_or("Untitled");
                format!("{tp} - {title}")
            }
            _ => at.track.title.as_deref().unwrap_or("Untitled").to_string(),
        };

        out.push_str(&format!(
            "  {:>width$}: {track_line} {dur_str}\n",
            at.track.number,
            width = num_width + 2,
        ));
    }

    if let Some(id) = disc_id {
        out.push_str(&format!("\n  discid: {id}\n"));
    }

    out
}

/// Format a CUEtools-style TOC.
pub fn format_cuetools_toc(sheet: &CueSheet, file_durations: &[u64]) -> String {
    let mut out = String::new();
    let abs_tracks = absolute_audio_tracks(sheet, file_durations);

    for (i, at) in abs_tracks.iter().enumerate() {
        let start_sector = at.absolute_offset;
        let start_time = CueTime::from_sectors(start_sector);
        let duration = abs_track_duration(at, abs_tracks.get(i + 1), file_durations);
        let dur_str = duration.map_or("??:??:??".to_string(), |d| format!("{d}"));
        let end_sector = match duration {
            Some(d) => format!("{:>6}", start_sector + d.to_sectors()),
            None => " ?????".into(),
        };

        out.push_str(&format!(
            "  {:>2} | {start_time} | {dur_str} | {start_sector:>6} | {end_sector}\n",
            at.track.number,
        ));
    }
    out
}

/// Format an EAC-style TOC (track listing portion only).
pub fn format_eac_toc(sheet: &CueSheet, file_durations: &[u64]) -> String {
    let mut out = String::new();
    out.push_str("     Track |   Start  |  Length  | Start Sector | End Sector\n");
    out.push_str("    ---------------------------------------------------------\n");

    let abs_tracks = absolute_audio_tracks(sheet, file_durations);

    for (i, at) in abs_tracks.iter().enumerate() {
        let start_sector = at.absolute_offset;
        let start_time = CueTime::from_sectors(start_sector);
        let duration = abs_track_duration(at, abs_tracks.get(i + 1), file_durations);
        let dur_str = duration.map_or("??:??:??".to_string(), |d| format!("{d}"));
        let end_sector = match duration {
            Some(d) => {
                let end = start_sector + d.to_sectors();
                format!("{:>10}", if end > 0 { end - 1 } else { 0 })
            }
            None => "     ?????".into(),
        };

        out.push_str(&format!(
            "    {:>5}  | {start_time} | {dur_str} | {:>12} | {end_sector}\n",
            at.track.number, start_sector,
        ));
    }
    out
}

/// An audio track with its absolute sector offset.
struct AbsoluteTrack<'a> {
    track: &'a CueTrack,
    absolute_offset: u64,
}

/// Build a list of audio tracks with absolute offsets computed from
/// per-file durations.
fn absolute_audio_tracks<'a>(
    sheet: &'a CueSheet,
    file_durations: &[u64],
) -> Vec<AbsoluteTrack<'a>> {
    let (offsets, _) = sheet.absolute_offsets(file_durations);
    let audio_tracks: Vec<&CueTrack> = sheet.tracks().filter(|t| t.track_type == "AUDIO").collect();

    audio_tracks
        .into_iter()
        .zip(offsets)
        .map(|(track, offset)| AbsoluteTrack {
            track,
            absolute_offset: offset,
        })
        .collect()
}

/// Compute duration from absolute offsets. For the last track, uses the
/// total disc duration from file_durations.
fn abs_track_duration(
    current: &AbsoluteTrack,
    next: Option<&AbsoluteTrack>,
    file_durations: &[u64],
) -> Option<CueTime> {
    if let Some(next) = next {
        let dur = next.absolute_offset.saturating_sub(current.absolute_offset);
        Some(CueTime::from_sectors(dur))
    } else if !file_durations.is_empty() {
        let total: u64 = file_durations.iter().sum();
        let dur = total.saturating_sub(current.absolute_offset);
        Some(CueTime::from_sectors(dur))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse_cue_sheet;

    const SAMPLE: &str = r#"REM GENRE Electronic
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
    fn test_album_summary() {
        let sheet = parse_cue_sheet(SAMPLE).unwrap();
        let total = CueTime::new(13, 52, 60).to_sectors();
        let summary = format_album_summary(&sheet, &[total], Some("test_id_123"));
        assert!(summary.contains("[1998] Various Artists: Rebirth of Cool, Volume 4 (Electronic)"));
        assert!(summary.contains("Kruder & Dorfmeister - Bug Powder Dust"));
        assert!(summary.contains("DJ Cam - Innervisions"));
        assert!(summary.contains("Thievery Corporation - Lebanese Blonde"));
        assert!(summary.contains("discid: test_id_123"));
    }

    #[test]
    fn test_album_summary_same_artist() {
        let cue = r#"PERFORMER "Portishead"
TITLE "Third"
FILE "third.flac" WAVE
  TRACK 01 AUDIO
    TITLE "Silence"
    PERFORMER "Portishead"
    INDEX 01 00:00:00
  TRACK 02 AUDIO
    TITLE "Hunter"
    PERFORMER "Portishead"
    INDEX 01 05:00:00
"#;
        let sheet = parse_cue_sheet(cue).unwrap();
        let summary = format_album_summary(&sheet, &[36000], None);
        assert!(summary.contains("Silence"));
        assert!(!summary.contains("Portishead - Silence"));
    }

    #[test]
    fn test_album_summary_no_durations() {
        let sheet = parse_cue_sheet(SAMPLE).unwrap();
        let summary = format_album_summary(&sheet, &[], None);
        // All tracks should show unknown duration since we have no file durations
        assert!(summary.contains("(??:??:??)"));
    }

    #[test]
    fn test_cuetools_toc() {
        let sheet = parse_cue_sheet(SAMPLE).unwrap();
        let total = CueTime::new(13, 52, 60).to_sectors();
        let toc = format_cuetools_toc(&sheet, &[total]);
        assert!(toc.contains("00:00:00"));
        assert!(toc.contains("04:35:12"));
        assert!(toc.contains("|      0 |"));
    }

    #[test]
    fn test_eac_toc() {
        let sheet = parse_cue_sheet(SAMPLE).unwrap();
        let total = CueTime::new(13, 52, 60).to_sectors();
        let toc = format_eac_toc(&sheet, &[total]);
        assert!(toc.contains("Track |   Start  |  Length  | Start Sector | End Sector"));
        assert!(toc.contains("-----"));
        assert!(toc.contains("00:00:00"));
    }

    #[test]
    fn test_multi_file_durations() {
        let cue = r#"PERFORMER "Test"
TITLE "Multi-File"
FILE "disc1_track1.wav" WAVE
  TRACK 01 AUDIO
    TITLE "Track 1"
    INDEX 01 00:00:00
FILE "disc1_track2.wav" WAVE
  TRACK 02 AUDIO
    TITLE "Track 2"
    INDEX 01 00:00:00
FILE "disc1_track3.wav" WAVE
  TRACK 03 AUDIO
    TITLE "Track 3"
    INDEX 01 00:00:00
"#;
        let sheet = parse_cue_sheet(cue).unwrap();
        // Each file is 3 minutes = 13500 sectors
        let durations = &[13500, 13500, 13500];
        let toc = format_cuetools_toc(&sheet, durations);

        // Verify absolute offsets appear in the TOC
        let lines: Vec<&str> = toc.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("0")); // track 1 starts at sector 0
        assert!(lines[1].contains("13500")); // track 2 at accumulated offset
        assert!(lines[2].contains("27000")); // track 3 at accumulated offset
    }
}

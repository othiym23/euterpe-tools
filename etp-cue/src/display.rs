use crate::types::{CueSheet, CueTime, CueTrack};

/// Format an album summary matching the mockup in docs/album-display-mockup.txt.
///
/// Durations are in MM:SS:FF (native CUE format). If `disc_id` is provided,
/// it's shown at the bottom. `total_sectors` is needed to compute the last
/// track's duration.
pub fn format_album_summary(
    sheet: &CueSheet,
    total_sectors: Option<u64>,
    disc_id: Option<&str>,
) -> String {
    let mut out = String::new();

    // Header: [YEAR] ARTIST: TITLE (GENRE)
    let year = sheet.date.as_deref().unwrap_or("????");
    let artist = sheet.performer.as_deref().unwrap_or("Unknown Artist");
    let title = sheet.title.as_deref().unwrap_or("Unknown Album");
    out.push_str(&format!("  [{year}] {artist}: {title}"));
    if let Some(genre) = &sheet.genre {
        out.push_str(&format!(" ({genre})"));
    }
    out.push_str("\n\n");

    // Track listing
    let tracks: Vec<&CueTrack> = sheet.tracks().collect();
    let num_width = if tracks.len() >= 10 { 2 } else { 1 };

    for (i, track) in tracks.iter().enumerate() {
        let duration = track_duration(track, tracks.get(i + 1).copied(), total_sectors);
        let dur_str = match duration {
            Some(d) => format!("({d})"),
            None => "(??:??:??)".into(),
        };

        // Show artist prefix only if track artist differs from album artist
        let track_line = match (&track.performer, &sheet.performer) {
            (Some(tp), Some(ap)) if tp != ap => {
                let title = track.title.as_deref().unwrap_or("Untitled");
                format!("{tp} - {title}")
            }
            _ => track.title.as_deref().unwrap_or("Untitled").to_string(),
        };

        out.push_str(&format!(
            "  {:>width$}: {track_line} {dur_str}\n",
            track.number,
            width = num_width + 2, // indent
        ));
    }

    if let Some(id) = disc_id {
        out.push_str(&format!("\n  discid: {id}\n"));
    }

    out
}

/// Format a CUEtools-style TOC.
///
/// Columns: track | start time | duration | start sector | end sector
pub fn format_cuetools_toc(sheet: &CueSheet, total_sectors: Option<u64>) -> String {
    let mut out = String::new();
    let tracks: Vec<&CueTrack> = sheet.tracks().collect();

    for (i, track) in tracks.iter().enumerate() {
        let start = track.index01;
        let start_sector = start.to_sectors();
        let duration = track_duration(track, tracks.get(i + 1).copied(), total_sectors);
        let dur_str = duration.map_or("??:??:??".to_string(), |d| format!("{d}"));
        let end_sector = match duration {
            Some(d) => format!("{:>6}", start_sector + d.to_sectors()),
            None => " ?????".into(),
        };

        out.push_str(&format!(
            "  {:>2} | {start} | {dur_str} | {start_sector:>6} | {end_sector}\n",
            track.number,
        ));
    }
    out
}

/// Format an EAC-style TOC (track listing portion only).
///
/// Columns: Track | Start | Length | Start Sector | End Sector
pub fn format_eac_toc(sheet: &CueSheet, total_sectors: Option<u64>) -> String {
    let mut out = String::new();
    out.push_str("     Track |   Start  |  Length  | Start Sector | End Sector\n");
    out.push_str("    ---------------------------------------------------------\n");

    let tracks: Vec<&CueTrack> = sheet.tracks().collect();

    for (i, track) in tracks.iter().enumerate() {
        let start = track.index01;
        let start_sector = start.to_sectors();
        let duration = track_duration(track, tracks.get(i + 1).copied(), total_sectors);
        let dur_str = duration.map_or("??:??:??".to_string(), |d| format!("{d}"));
        let end_sector = match duration {
            Some(d) => {
                let end = start_sector + d.to_sectors();
                format!("{:>10}", if end > 0 { end - 1 } else { 0 })
            }
            None => "     ?????".into(),
        };

        out.push_str(&format!(
            "    {:>5}  | {start} | {dur_str} | {:>12} | {end_sector}\n",
            track.number, start_sector,
        ));
    }
    out
}

/// Compute a track's duration from the gap between its INDEX 01 and the next
/// track's INDEX 01 (or the total disc length for the last track).
fn track_duration(
    track: &CueTrack,
    next_track: Option<&CueTrack>,
    total_sectors: Option<u64>,
) -> Option<CueTime> {
    let start = track.index01;
    if let Some(next) = next_track {
        Some(CueTime::duration_between(start, next.index01))
    } else {
        // Last track: need total duration
        total_sectors.map(|total| CueTime::duration_between(start, CueTime::from_sectors(total)))
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
        let summary = format_album_summary(&sheet, Some(total), Some("test_id_123"));
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
        let summary = format_album_summary(&sheet, Some(36000), None);
        // Same artist as album — should NOT show "Portishead - Silence"
        assert!(summary.contains("Silence"));
        assert!(!summary.contains("Portishead - Silence"));
    }

    #[test]
    fn test_album_summary_no_total() {
        let sheet = parse_cue_sheet(SAMPLE).unwrap();
        let summary = format_album_summary(&sheet, None, None);
        // Last track should show unknown duration
        assert!(summary.contains("(??:??:??)"));
        // First two tracks can still compute duration
        assert!(!summary.starts_with("??"));
    }

    #[test]
    fn test_cuetools_toc() {
        let sheet = parse_cue_sheet(SAMPLE).unwrap();
        let total = CueTime::new(13, 52, 60).to_sectors();
        let toc = format_cuetools_toc(&sheet, Some(total));
        assert!(toc.contains("00:00:00"));
        assert!(toc.contains("04:35:12"));
        // Track 1 start sector = 0
        assert!(toc.contains("|      0 |"));
    }

    #[test]
    fn test_eac_toc() {
        let sheet = parse_cue_sheet(SAMPLE).unwrap();
        let total = CueTime::new(13, 52, 60).to_sectors();
        let toc = format_eac_toc(&sheet, Some(total));
        assert!(toc.contains("Track |   Start  |  Length  | Start Sector | End Sector"));
        assert!(toc.contains("-----"));
        assert!(toc.contains("00:00:00"));
    }
}

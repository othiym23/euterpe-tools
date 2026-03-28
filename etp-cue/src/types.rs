use std::fmt;

/// A parsed CUE sheet.
#[derive(Debug, Clone)]
pub struct CueSheet {
    pub performer: Option<String>,
    pub title: Option<String>,
    pub genre: Option<String>,
    pub date: Option<String>,
    pub catalog: Option<String>,
    pub files: Vec<CueFile>,
}

impl CueSheet {
    /// Iterate all tracks across all files in order.
    pub fn tracks(&self) -> impl Iterator<Item = &CueTrack> {
        self.files.iter().flat_map(|f| f.tracks.iter())
    }

    /// Count of audio tracks.
    pub fn track_count(&self) -> usize {
        self.tracks().filter(|t| t.track_type == "AUDIO").count()
    }

    /// Compute absolute sector offsets for all audio tracks given per-file
    /// durations in sectors. Returns `(absolute_offsets, total_sectors)`.
    ///
    /// For single-file CUE sheets, pass `&[total_sectors]`.
    /// For multi-file CUE sheets, pass one duration per FILE block.
    pub fn absolute_offsets(&self, file_durations: &[u64]) -> (Vec<u64>, u64) {
        let mut offsets = Vec::new();
        let mut cumulative: u64 = 0;

        for (file_idx, file) in self.files.iter().enumerate() {
            for track in &file.tracks {
                if track.track_type == "AUDIO" {
                    offsets.push(cumulative + track.index01.to_sectors());
                }
            }
            if file_idx < file_durations.len() {
                cumulative += file_durations[file_idx];
            }
        }

        (offsets, cumulative)
    }
}

/// A FILE block within a CUE sheet.
#[derive(Debug, Clone)]
pub struct CueFile {
    pub filename: String,
    pub file_type: String,
    pub tracks: Vec<CueTrack>,
}

/// A TRACK within a FILE block.
#[derive(Debug, Clone)]
pub struct CueTrack {
    pub number: u32,
    pub track_type: String,
    pub title: Option<String>,
    pub performer: Option<String>,
    pub isrc: Option<String>,
    pub pregap: Option<CueTime>,
    pub index00: Option<CueTime>,
    pub index01: CueTime,
    pub postgap: Option<CueTime>,
}

/// Convert milliseconds to CD sectors (75 sectors/second), with rounding.
pub fn milliseconds_to_sectors(ms: u64) -> u64 {
    (ms * 75 + 500) / 1000
}

/// Time in CUE format: MM:SS:FF where FF = frames at 75 fps.
/// One frame = one CD sector = 1/75 second.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CueTime {
    pub minutes: u32,
    pub seconds: u32,
    pub frames: u32,
}

impl CueTime {
    pub const ZERO: CueTime = CueTime {
        minutes: 0,
        seconds: 0,
        frames: 0,
    };

    pub fn new(minutes: u32, seconds: u32, frames: u32) -> Self {
        Self {
            minutes,
            seconds,
            frames,
        }
    }

    /// Convert to absolute sector count.
    pub fn to_sectors(self) -> u64 {
        (self.minutes as u64) * 60 * 75 + (self.seconds as u64) * 75 + (self.frames as u64)
    }

    /// Create from an absolute sector count.
    pub fn from_sectors(sectors: u64) -> Self {
        let frames = (sectors % 75) as u32;
        let total_seconds = sectors / 75;
        let seconds = (total_seconds % 60) as u32;
        let minutes = (total_seconds / 60) as u32;
        Self {
            minutes,
            seconds,
            frames,
        }
    }

    /// Compute the duration between two times (end - start).
    pub fn duration_between(start: CueTime, end: CueTime) -> CueTime {
        let start_sectors = start.to_sectors();
        let end_sectors = end.to_sectors();
        if end_sectors > start_sectors {
            CueTime::from_sectors(end_sectors - start_sectors)
        } else {
            CueTime::ZERO
        }
    }
}

impl fmt::Display for CueTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:02}:{:02}:{:02}",
            self.minutes, self.seconds, self.frames
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_sectors() {
        assert_eq!(CueTime::new(0, 0, 0).to_sectors(), 0);
        assert_eq!(CueTime::new(0, 0, 1).to_sectors(), 1);
        assert_eq!(CueTime::new(0, 1, 0).to_sectors(), 75);
        assert_eq!(CueTime::new(1, 0, 0).to_sectors(), 4500);
        assert_eq!(CueTime::new(0, 2, 0).to_sectors(), 150); // standard pregap
        assert_eq!(CueTime::new(4, 35, 12).to_sectors(), 20637);
    }

    #[test]
    fn test_from_sectors() {
        assert_eq!(CueTime::from_sectors(0), CueTime::new(0, 0, 0));
        assert_eq!(CueTime::from_sectors(150), CueTime::new(0, 2, 0));
        assert_eq!(CueTime::from_sectors(20637), CueTime::new(4, 35, 12));
    }

    #[test]
    fn test_roundtrip() {
        for sectors in [0, 1, 74, 75, 150, 4500, 20637, 164900] {
            assert_eq!(CueTime::from_sectors(sectors).to_sectors(), sectors);
        }
    }

    #[test]
    fn test_duration_between() {
        let start = CueTime::new(0, 0, 0);
        let end = CueTime::new(4, 35, 12);
        assert_eq!(CueTime::duration_between(start, end), end);

        let mid = CueTime::new(2, 0, 0);
        let dur = CueTime::duration_between(mid, end);
        assert_eq!(dur.to_sectors(), end.to_sectors() - mid.to_sectors());
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", CueTime::new(0, 0, 0)), "00:00:00");
        assert_eq!(format!("{}", CueTime::new(4, 35, 12)), "04:35:12");
        assert_eq!(format!("{}", CueTime::new(72, 5, 3)), "72:05:03");
    }
}

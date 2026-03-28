use crate::types::CueSheet;
use base64::{Engine, engine::general_purpose::STANDARD};
use sha1::{Digest, Sha1};

/// Standard CD pregap: 2 seconds = 150 sectors.
const PREGAP_SECTORS: u64 = 150;

/// Compute a MusicBrainz disc ID from a parsed CUE sheet.
///
/// `file_durations` provides per-FILE sector counts. For a single-image CUE
/// sheet, pass `&[total_sectors]`. For multi-file CUE sheets, pass one
/// duration per FILE block.
///
/// Assumes `first_track = 1`, which is correct for standard audio CDs.
pub fn compute_disc_id(sheet: &CueSheet, file_durations: &[u64]) -> String {
    let (offsets, total_sectors) = sheet.absolute_offsets(file_durations);

    if offsets.is_empty() {
        return String::new();
    }

    let first_track: u8 = 1;
    let last_track: u8 = offsets.len() as u8;
    let lead_out: u32 = (total_sectors + PREGAP_SECTORS) as u32;

    // SHA-1 input: first_track(2) + last_track(2) + lead_out(8) + 99 offsets(8 each)
    let mut hash_input = String::with_capacity(804);
    hash_input.push_str(&format!("{first_track:02X}"));
    hash_input.push_str(&format!("{last_track:02X}"));
    hash_input.push_str(&format!("{lead_out:08X}"));

    for i in 0..99usize {
        if i < offsets.len() {
            let offset = (offsets[i] + PREGAP_SECTORS) as u32;
            hash_input.push_str(&format!("{offset:08X}"));
        } else {
            hash_input.push_str("00000000");
        }
    }

    let mut hasher = Sha1::new();
    hasher.update(hash_input.as_bytes());
    let digest = hasher.finalize();

    // MusicBrainz uses a URL-safe Base64 variant that replaces +, /, and =
    // with ., _, and - respectively.
    STANDARD
        .encode(digest)
        .chars()
        .map(|c| match c {
            '+' => '.',
            '/' => '_',
            '=' => '-',
            c => c,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse_cue_sheet;

    #[test]
    fn test_disc_id_format() {
        let cue = r#"FILE "test.wav" WAVE
  TRACK 01 AUDIO
    INDEX 01 00:00:00
  TRACK 02 AUDIO
    INDEX 01 04:35:12
"#;
        let sheet = parse_cue_sheet(cue).unwrap();
        let id = compute_disc_id(&sheet, &[45000]);
        assert_eq!(id.len(), 28, "disc ID should be 28 characters");
        assert!(
            id.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-'),
            "disc ID contains invalid characters: {id}"
        );
    }

    #[test]
    fn test_disc_id_deterministic() {
        let cue = r#"FILE "test.wav" WAVE
  TRACK 01 AUDIO
    INDEX 01 00:00:00
"#;
        let sheet = parse_cue_sheet(cue).unwrap();
        let id1 = compute_disc_id(&sheet, &[30000]);
        let id2 = compute_disc_id(&sheet, &[30000]);
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_disc_id_empty_sheet() {
        let sheet = CueSheet {
            performer: None,
            title: None,
            genre: None,
            date: None,
            catalog: None,
            files: Vec::new(),
        };
        assert_eq!(compute_disc_id(&sheet, &[]), "");
    }

    /// Test against libdiscid's official test vector (test_put.c).
    /// 22-track CD with known disc ID "xUp1F2NkfP8s8jaeFn_Av3jNEI4-".
    #[test]
    fn test_disc_id_libdiscid_vector() {
        // The test vector provides absolute offsets including the 150-sector
        // pregap. Our CUE sheet offsets are WITHOUT pregap (the algorithm adds
        // it), so subtract 150 from each test vector offset.
        let track_offsets: Vec<u64> = vec![
            150, 9700, 25887, 39297, 53795, 63735, 77517, 94877, 107270, 123552, 135522, 148422,
            161197, 174790, 192022, 205545, 218010, 228700, 239590, 255470, 266932, 288750,
        ];
        let lead_out: u64 = 303602;

        // Build a CUE sheet with these offsets (subtract pregap since our
        // code adds it back)
        let mut cue = String::from("FILE \"test.wav\" WAVE\n");
        for (i, &offset) in track_offsets.iter().enumerate() {
            let adjusted = offset - PREGAP_SECTORS;
            let time = crate::types::CueTime::from_sectors(adjusted);
            cue.push_str(&format!(
                "  TRACK {:02} AUDIO\n    INDEX 01 {time}\n",
                i + 1
            ));
        }
        let sheet = parse_cue_sheet(&cue).unwrap();

        // total_sectors = lead_out - pregap (our code adds pregap to get lead_out)
        let total = lead_out - PREGAP_SECTORS;
        let id = compute_disc_id(&sheet, &[total]);
        assert_eq!(id, "xUp1F2NkfP8s8jaeFn_Av3jNEI4-");
    }

    /// Minimal 1-track test vector from python-discid.
    #[test]
    fn test_disc_id_single_track() {
        let cue = r#"FILE "test.wav" WAVE
  TRACK 01 AUDIO
    INDEX 01 00:00:00
"#;
        let sheet = parse_cue_sheet(cue).unwrap();
        let total = 44942 - PREGAP_SECTORS; // lead_out - pregap
        let id = compute_disc_id(&sheet, &[total]);
        assert_eq!(id, "ANJa4DGYN_ktpzOwvVPtcjwP7mE-");
    }
}

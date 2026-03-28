use crate::types::CueSheet;
use sha1::{Digest, Sha1};

/// Standard CD pregap: 2 seconds = 150 sectors.
const PREGAP_SECTORS: u64 = 150;

/// MusicBrainz custom Base64 alphabet replacements.
const MB_BASE64_REPLACE: [(char, char); 3] = [('+', '.'), ('/', '_'), ('=', '-')];

/// Compute a MusicBrainz disc ID from a parsed CUE sheet.
///
/// `total_sectors` is the total length of the audio in CD sectors (75 per second),
/// needed to compute the lead-out position.
pub fn compute_disc_id(sheet: &CueSheet, total_sectors: u64) -> String {
    let audio_tracks: Vec<&crate::types::CueTrack> =
        sheet.tracks().filter(|t| t.track_type == "AUDIO").collect();

    if audio_tracks.is_empty() {
        return String::new();
    }

    let first_track: u8 = 1;
    let last_track: u8 = audio_tracks.len() as u8;
    let lead_out: u32 = (total_sectors + PREGAP_SECTORS) as u32;

    // Build the hash input string
    let mut hash_input = String::new();
    hash_input.push_str(&format!("{first_track:02X}"));
    hash_input.push_str(&format!("{last_track:02X}"));
    hash_input.push_str(&format!("{lead_out:08X}"));

    // 99 track offset slots
    for i in 0..99u8 {
        if i < audio_tracks.len() as u8 {
            let offset = audio_tracks[i as usize].index01.to_sectors() + PREGAP_SECTORS;
            hash_input.push_str(&format!("{:08X}", offset as u32));
        } else {
            hash_input.push_str("00000000");
        }
    }

    // SHA-1 hash
    let mut hasher = Sha1::new();
    hasher.update(hash_input.as_bytes());
    let digest = hasher.finalize();

    // Base64 encode with MusicBrainz alphabet
    let b64 = base64_encode(&digest);
    let mut result = b64;
    for (from, to) in &MB_BASE64_REPLACE {
        result = result.replace(*from, &to.to_string());
    }
    result
}

fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut result = String::new();
    let chunks = data.chunks(3);
    for chunk in chunks {
        let (b0, b1, b2) = match chunk.len() {
            3 => (chunk[0], chunk[1], chunk[2]),
            2 => (chunk[0], chunk[1], 0),
            1 => (chunk[0], 0, 0),
            _ => unreachable!(),
        };

        let n = ((b0 as u32) << 16) | ((b1 as u32) << 8) | (b2 as u32);
        result.push(ALPHABET[((n >> 18) & 0x3F) as usize] as char);
        result.push(ALPHABET[((n >> 12) & 0x3F) as usize] as char);

        if chunk.len() > 1 {
            result.push(ALPHABET[((n >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(ALPHABET[(n & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
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
        // 10 minutes total = 10 * 60 * 75 = 45000 sectors
        let id = compute_disc_id(&sheet, 45000);
        assert_eq!(id.len(), 28, "disc ID should be 28 characters");
        // Should only contain MusicBrainz-safe characters
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
        let id1 = compute_disc_id(&sheet, 30000);
        let id2 = compute_disc_id(&sheet, 30000);
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_disc_id_changes_with_offsets() {
        let cue1 = r#"FILE "test.wav" WAVE
  TRACK 01 AUDIO
    INDEX 01 00:00:00
"#;
        let cue2 = r#"FILE "test.wav" WAVE
  TRACK 01 AUDIO
    INDEX 01 00:01:00
"#;
        let sheet1 = parse_cue_sheet(cue1).unwrap();
        let sheet2 = parse_cue_sheet(cue2).unwrap();
        assert_ne!(
            compute_disc_id(&sheet1, 30000),
            compute_disc_id(&sheet2, 30000)
        );
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
        assert_eq!(compute_disc_id(&sheet, 0), "");
    }

    #[test]
    fn test_base64_encode_known_value() {
        // "Man" should encode to "TWFu"
        assert_eq!(base64_encode(b"Man"), "TWFu");
        // Single byte with padding
        assert_eq!(base64_encode(b"M"), "TQ==");
    }
}

use lofty::prelude::*;
use lofty::probe::Probe;
use lofty::tag::ItemKey;
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;

/// Audio file extensions recognized for metadata scanning.
pub const AUDIO_EXTENSIONS: &[&str] = &[
    "aac", "aiff", "ape", "flac", "m4a", "mp3", "mpc", "ogg", "opus", "spx", "wav", "wv",
];

/// Check whether a filename has an audio extension.
pub fn is_audio_file(filename: &str) -> bool {
    filename
        .rsplit('.')
        .next()
        .is_some_and(|ext| AUDIO_EXTENSIONS.contains(&ext.to_ascii_lowercase().as_str()))
}

/// All metadata extracted from a single audio file.
#[derive(Debug, Serialize)]
pub struct FileMetadata {
    pub properties: Vec<(String, serde_json::Value)>,
    pub tags: Vec<(String, serde_json::Value)>,
    pub images: Vec<ImageMetadata>,
    pub cue_sheet: Option<String>,
}

/// Metadata about an embedded image.
#[derive(Debug, Serialize)]
pub struct ImageMetadata {
    pub image_type: String,
    pub mime_type: String,
    #[serde(skip)]
    pub data: Vec<u8>,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

/// Read all metadata from an audio file.
pub fn read_metadata(path: &Path) -> Result<FileMetadata, lofty::error::LoftyError> {
    let tagged_file = Probe::open(path)?.read()?;

    let properties = extract_properties(&tagged_file);

    let tag = tagged_file
        .primary_tag()
        .or_else(|| tagged_file.first_tag());

    let (tags, images, cue_sheet) = match tag {
        Some(t) => (extract_tags(t), extract_images(t), extract_cue_sheet(t)),
        None => (Vec::new(), Vec::new(), None),
    };

    Ok(FileMetadata {
        properties,
        tags,
        images,
        cue_sheet,
    })
}

fn extract_properties(file: &lofty::file::TaggedFile) -> Vec<(String, serde_json::Value)> {
    let props = file.properties();
    let mut out = Vec::new();

    let d = props.duration();
    if !d.is_zero() {
        out.push((
            "audio_duration_ms".into(),
            serde_json::Value::Number(serde_json::Number::from(d.as_millis() as u64)),
        ));
    }
    if let Some(b) = props.audio_bitrate() {
        out.push(("audio_bitrate".into(), serde_json::Value::Number(b.into())));
    }
    if let Some(s) = props.sample_rate() {
        out.push((
            "audio_sample_rate".into(),
            serde_json::Value::Number(s.into()),
        ));
    }
    if let Some(d) = props.bit_depth() {
        out.push((
            "audio_bit_depth".into(),
            serde_json::Value::Number(d.into()),
        ));
    }
    if let Some(c) = props.channels() {
        out.push(("audio_channels".into(), serde_json::Value::Number(c.into())));
    }
    out
}

fn extract_tags(tag: &lofty::tag::Tag) -> Vec<(String, serde_json::Value)> {
    let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
    for item in tag.items() {
        let key = normalize_item_key(&item.key()).into_owned();
        if let Some(val) = item.value().text() {
            grouped.entry(key).or_default().push(val.to_string());
        }
    }

    let mut out: Vec<(String, serde_json::Value)> = grouped
        .into_iter()
        .map(|(key, values)| {
            let json_val = if values.len() == 1 {
                serde_json::Value::String(values.into_iter().next().unwrap())
            } else {
                serde_json::Value::Array(
                    values.into_iter().map(serde_json::Value::String).collect(),
                )
            };
            (key, json_val)
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn extract_images(tag: &lofty::tag::Tag) -> Vec<ImageMetadata> {
    tag.pictures()
        .iter()
        .map(|pic| {
            let mime_str = match pic.mime_type() {
                Some(mt) => format!("{mt}"),
                None => "application/octet-stream".into(),
            };
            ImageMetadata {
                image_type: normalize_picture_type(&pic.pic_type()).into_owned(),
                mime_type: mime_str,
                data: pic.data().to_vec(),
                width: None,
                height: None,
            }
        })
        .collect()
}

fn extract_cue_sheet(tag: &lofty::tag::Tag) -> Option<String> {
    // FLAC stores cue sheets as a Vorbis comment with key "CUESHEET".
    // Iterate items and check the format-specific key since lofty has no
    // built-in ItemKey variant for cue sheets.
    tag.items()
        .find(|item| item.key().map_key(tag.tag_type()) == Some("CUESHEET"))
        .and_then(|item| item.value().text().map(|s| s.to_string()))
}

/// Normalize a lofty `ItemKey` to `lowercase_snake_case`.
/// Returns a static string for known keys to avoid allocations.
pub fn normalize_item_key(key: &ItemKey) -> std::borrow::Cow<'static, str> {
    match key {
        ItemKey::TrackTitle => "track_title".into(),
        ItemKey::TrackArtist => "track_artist".into(),
        ItemKey::TrackNumber => "track_number".into(),
        ItemKey::TrackTotal => "track_total".into(),
        ItemKey::AlbumTitle => "album_title".into(),
        ItemKey::AlbumArtist => "album_artist".into(),
        ItemKey::DiscNumber => "disc_number".into(),
        ItemKey::DiscTotal => "disc_total".into(),
        ItemKey::Genre => "genre".into(),
        ItemKey::Year => "year".into(),
        ItemKey::RecordingDate => "recording_date".into(),
        ItemKey::Comment => "comment".into(),
        ItemKey::Composer => "composer".into(),
        ItemKey::Conductor => "conductor".into(),
        ItemKey::Lyrics => "lyrics".into(),
        ItemKey::EncoderSoftware => "encoder_software".into(),
        ItemKey::EncodedBy => "encoded_by".into(),
        ItemKey::CopyrightMessage => "copyright".into(),
        ItemKey::Label => "label".into(),
        ItemKey::CatalogNumber => "catalog_number".into(),
        ItemKey::Barcode => "barcode".into(),
        ItemKey::Isrc => "isrc".into(),
        ItemKey::Mood => "mood".into(),
        ItemKey::Language => "language".into(),
        ItemKey::Bpm => "bpm".into(),
        ItemKey::ReplayGainAlbumGain => "replaygain_album_gain".into(),
        ItemKey::ReplayGainAlbumPeak => "replaygain_album_peak".into(),
        ItemKey::ReplayGainTrackGain => "replaygain_track_gain".into(),
        ItemKey::ReplayGainTrackPeak => "replaygain_track_peak".into(),
        ItemKey::MusicBrainzRecordingId => "musicbrainz_recording_id".into(),
        ItemKey::MusicBrainzReleaseId => "musicbrainz_release_id".into(),
        ItemKey::MusicBrainzReleaseGroupId => "musicbrainz_release_group_id".into(),
        ItemKey::MusicBrainzArtistId => "musicbrainz_artist_id".into(),
        ItemKey::MusicBrainzReleaseArtistId => "musicbrainz_release_artist_id".into(),
        ItemKey::MusicBrainzTrackId => "musicbrainz_track_id".into(),
        ItemKey::MusicBrainzWorkId => "musicbrainz_work_id".into(),
        _ => format!("unknown_{:?}", key).to_ascii_lowercase().into(),
    }
}

/// Normalize a lofty `PictureType` to a snake_case string.
pub fn normalize_picture_type(pt: &lofty::picture::PictureType) -> std::borrow::Cow<'static, str> {
    use lofty::picture::PictureType;
    match pt {
        PictureType::Other => "other".into(),
        PictureType::Icon => "icon".into(),
        PictureType::OtherIcon => "other_icon".into(),
        PictureType::CoverFront => "front_cover".into(),
        PictureType::CoverBack => "back_cover".into(),
        PictureType::Leaflet => "leaflet".into(),
        PictureType::Media => "media".into(),
        PictureType::LeadArtist => "lead_artist".into(),
        PictureType::Artist => "artist".into(),
        PictureType::Conductor => "conductor".into(),
        PictureType::Band => "band".into(),
        PictureType::Composer => "composer".into(),
        PictureType::Lyricist => "lyricist".into(),
        PictureType::RecordingLocation => "recording_location".into(),
        PictureType::DuringRecording => "during_recording".into(),
        PictureType::DuringPerformance => "during_performance".into(),
        PictureType::ScreenCapture => "screen_capture".into(),
        PictureType::BrightFish => "bright_fish".into(),
        PictureType::Illustration => "illustration".into(),
        PictureType::BandLogo => "band_logo".into(),
        PictureType::PublisherLogo => "publisher_logo".into(),
        _ => format!("unknown_{:?}", pt).to_ascii_lowercase().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_audio_file() {
        assert!(is_audio_file("song.mp3"));
        assert!(is_audio_file("song.FLAC"));
        assert!(is_audio_file("path/to/song.ogg"));
        assert!(is_audio_file("track.m4a"));
        assert!(!is_audio_file("video.mkv"));
        assert!(!is_audio_file("image.png"));
        assert!(!is_audio_file("noextension"));
        assert!(!is_audio_file(""));
    }

    #[test]
    fn test_normalize_item_key_known() {
        assert_eq!(normalize_item_key(&ItemKey::TrackTitle), "track_title");
        assert_eq!(normalize_item_key(&ItemKey::AlbumArtist), "album_artist");
        assert_eq!(normalize_item_key(&ItemKey::Genre), "genre");
        assert_eq!(
            normalize_item_key(&ItemKey::ReplayGainTrackGain),
            "replaygain_track_gain"
        );
        assert_eq!(
            normalize_item_key(&ItemKey::MusicBrainzRecordingId),
            "musicbrainz_recording_id"
        );
    }

    #[test]
    fn test_normalize_item_key_fallback() {
        // Unknown variants use the Debug representation
        let key = ItemKey::FlagCompilation;
        let result = normalize_item_key(&key);
        assert!(result.starts_with("unknown_"), "got: {result}");
    }

    #[test]
    fn test_normalize_picture_type() {
        use lofty::picture::PictureType;
        assert_eq!(
            normalize_picture_type(&PictureType::CoverFront),
            "front_cover"
        );
        assert_eq!(
            normalize_picture_type(&PictureType::CoverBack),
            "back_cover"
        );
        assert_eq!(normalize_picture_type(&PictureType::Other), "other");
        assert_eq!(normalize_picture_type(&PictureType::BandLogo), "band_logo");
    }
}

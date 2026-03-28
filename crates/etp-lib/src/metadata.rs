use lofty::prelude::*;
use lofty::probe::Probe;
use lofty::tag::ItemKey;
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;

/// Audio file extensions recognized for metadata scanning.
pub const AUDIO_EXTENSIONS: &[&str] = &[
    "aac", "aiff", "ape", "dsf", "flac", "m4a", "mka", "mp3", "mpc", "ogg", "opus", "spx", "wav",
    "wma", "wv",
];

/// Extensions that require the mediainfo subprocess instead of lofty.
const MEDIAINFO_EXTENSIONS: &[&str] = &["wma", "mka"];

/// Check whether a filename has an audio extension.
pub fn is_audio_file(filename: &str) -> bool {
    filename
        .rsplit('.')
        .next()
        .is_some_and(|ext| AUDIO_EXTENSIONS.contains(&ext.to_ascii_lowercase().as_str()))
}

/// Errors from metadata reading.
#[derive(Debug)]
pub enum MetadataError {
    Lofty(lofty::error::LoftyError),
    Io(std::io::Error),
    MediainfoNotFound,
    MediainfoFailed(String),
    ParseError(String),
}

impl std::fmt::Display for MetadataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lofty(e) => write!(f, "{e}"),
            Self::Io(e) => write!(f, "{e}"),
            Self::MediainfoNotFound => write!(f, "mediainfo not found on PATH"),
            Self::MediainfoFailed(msg) => write!(f, "mediainfo failed: {msg}"),
            Self::ParseError(msg) => write!(f, "parse error: {msg}"),
        }
    }
}

impl std::error::Error for MetadataError {}

impl From<lofty::error::LoftyError> for MetadataError {
    fn from(e: lofty::error::LoftyError) -> Self {
        Self::Lofty(e)
    }
}

impl From<std::io::Error> for MetadataError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
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

/// Read all metadata from an audio file. Dispatches to lofty for most formats,
/// mediainfo subprocess for WMA and MKA.
pub fn read_metadata(path: &Path) -> Result<FileMetadata, MetadataError> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase())
        .unwrap_or_default();

    if MEDIAINFO_EXTENSIONS.contains(&ext.as_str()) {
        read_metadata_mediainfo(path)
    } else {
        read_metadata_lofty(path)
    }
}

// --- Lofty backend ---

fn read_metadata_lofty(path: &Path) -> Result<FileMetadata, MetadataError> {
    let tagged_file = Probe::open(path)?.read()?;

    let properties = extract_lofty_properties(&tagged_file);

    let tag = tagged_file
        .primary_tag()
        .or_else(|| tagged_file.first_tag());

    let (tags, images, cue_sheet) = match tag {
        Some(t) => (
            extract_lofty_tags(t),
            extract_lofty_images(t),
            extract_cue_sheet(t),
        ),
        None => (Vec::new(), Vec::new(), None),
    };

    Ok(FileMetadata {
        properties,
        tags,
        images,
        cue_sheet,
    })
}

fn extract_lofty_properties(file: &lofty::file::TaggedFile) -> Vec<(String, serde_json::Value)> {
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

fn extract_lofty_tags(tag: &lofty::tag::Tag) -> Vec<(String, serde_json::Value)> {
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

fn extract_lofty_images(tag: &lofty::tag::Tag) -> Vec<ImageMetadata> {
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
    tag.items()
        .find(|item| item.key().map_key(tag.tag_type()) == Some("CUESHEET"))
        .and_then(|item| item.value().text().map(|s| s.to_string()))
}

// --- Mediainfo backend ---

fn read_metadata_mediainfo(path: &Path) -> Result<FileMetadata, MetadataError> {
    let output = std::process::Command::new("mediainfo")
        .args(["--Output=JSON"])
        .arg(path)
        .output()
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                MetadataError::MediainfoNotFound
            } else {
                MetadataError::Io(e)
            }
        })?;

    if !output.status.success() {
        return Err(MetadataError::MediainfoFailed(
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ));
    }

    let json: serde_json::Value = serde_json::from_slice(&output.stdout)
        .map_err(|e| MetadataError::ParseError(e.to_string()))?;

    parse_mediainfo_json(&json)
}

fn parse_mediainfo_json(json: &serde_json::Value) -> Result<FileMetadata, MetadataError> {
    let tracks = json["media"]["track"]
        .as_array()
        .ok_or_else(|| MetadataError::ParseError("missing media.track array".into()))?;

    let general = tracks.iter().find(|t| t["@type"] == "General");
    let audio = tracks.iter().find(|t| t["@type"] == "Audio");

    let properties = extract_mediainfo_properties(audio);
    let tags = extract_mediainfo_tags(general);

    Ok(FileMetadata {
        properties,
        tags,
        images: Vec::new(),
        cue_sheet: None,
    })
}

fn extract_mediainfo_properties(
    audio: Option<&serde_json::Value>,
) -> Vec<(String, serde_json::Value)> {
    let mut out = Vec::new();
    let Some(audio) = audio else { return out };

    if let Some(d) = audio["Duration"]
        .as_str()
        .and_then(|s| s.parse::<f64>().ok())
    {
        out.push((
            "audio_duration_ms".into(),
            serde_json::Value::Number(serde_json::Number::from((d * 1000.0).round() as u64)),
        ));
    }
    if let Some(br) = audio["BitRate"]
        .as_str()
        .and_then(|s| s.parse::<u64>().ok())
    {
        out.push((
            "audio_bitrate".into(),
            serde_json::Value::Number((br / 1000).into()),
        ));
    }
    if let Some(sr) = audio["SamplingRate"]
        .as_str()
        .and_then(|s| s.parse::<u64>().ok())
    {
        out.push((
            "audio_sample_rate".into(),
            serde_json::Value::Number(sr.into()),
        ));
    }
    if let Some(bd) = audio["BitDepth"]
        .as_str()
        .and_then(|s| s.parse::<u64>().ok())
    {
        out.push((
            "audio_bit_depth".into(),
            serde_json::Value::Number(bd.into()),
        ));
    }
    if let Some(ch) = audio["Channels"]
        .as_str()
        .and_then(|s| s.parse::<u64>().ok())
    {
        out.push((
            "audio_channels".into(),
            serde_json::Value::Number(ch.into()),
        ));
    }
    out
}

fn extract_mediainfo_tags(general: Option<&serde_json::Value>) -> Vec<(String, serde_json::Value)> {
    let Some(general) = general else {
        return Vec::new();
    };
    let Some(obj) = general.as_object() else {
        return Vec::new();
    };

    // Mediainfo's General track mixes format/technical fields with user-facing
    // tags. Use an allowlist of known tag fields rather than a blocklist of
    // format fields, since mediainfo can output many undocumented technical
    // fields that would leak through a blocklist.
    let tag_fields = [
        "Title",
        "Track",
        "Performer",
        "Album",
        "Album/Performer",
        "Track/Position",
        "Track name/Position",
        "Track/Position_Total",
        "Part/Position",
        "Part/Position_Total",
        "Recorded_Date",
        "Recorded date",
        "Genre",
        "Comment",
        "Composer",
        "Conductor",
        "Lyricist",
        "Lyrics",
        "Publisher",
        "Label",
        "ISRC",
        "Barcode",
        "UPC",
        "CatalogNumber",
        "BPM",
        "Copyright",
        "ContentType",
        "Mood",
        "Language",
        "Description",
    ];

    // Use a HashMap to deduplicate: multiple mediainfo fields can map to the
    // same canonical key (e.g. Title and Track both → track_title). First
    // value wins.
    let mut seen: HashMap<String, serde_json::Value> = HashMap::new();
    for (key, val) in obj {
        if !tag_fields.contains(&key.as_str()) {
            continue;
        }
        if let Some(s) = val.as_str()
            && !s.is_empty()
        {
            let normalized = normalize_mediainfo_tag_key(key);
            seen.entry(normalized)
                .or_insert_with(|| serde_json::Value::String(s.to_string()));
        }
    }
    let mut out: Vec<(String, serde_json::Value)> = seen.into_iter().collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Map mediainfo General track field names to canonical tag names.
fn normalize_mediainfo_tag_key(key: &str) -> String {
    match key {
        "Title" => "track_title".into(),
        "Track" => "track_title".into(),
        "Performer" => "track_artist".into(),
        "Album" => "album_title".into(),
        "Album/Performer" => "album_artist".into(),
        "Track/Position" | "Track name/Position" => "track_number".into(),
        "Track/Position_Total" => "track_total".into(),
        "Part/Position" => "disc_number".into(),
        "Part/Position_Total" => "disc_total".into(),
        "Recorded_Date" | "Recorded date" => "year".into(),
        "Genre" => "genre".into(),
        "Comment" => "comment".into(),
        "Composer" => "composer".into(),
        "Conductor" => "conductor".into(),
        "Lyricist" => "lyricist".into(),
        "Lyrics" => "lyrics".into(),
        "Publisher" | "Label" => "label".into(),
        "ISRC" => "isrc".into(),
        "Barcode" | "UPC" => "barcode".into(),
        "CatalogNumber" => "catalog_number".into(),
        "BPM" => "bpm".into(),
        "Copyright" => "copyright".into(),
        "ContentType" => "content_type".into(),
        "Mood" => "mood".into(),
        "Language" => "language".into(),
        "Description" => "description".into(),
        other => other.to_ascii_lowercase().replace(['/', ' ', '-'], "_"),
    }
}

// --- Normalization (shared) ---

/// Normalize a lofty `ItemKey` to `lowercase_snake_case`.
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
        assert!(is_audio_file("album.dsf"));
        assert!(is_audio_file("track.wma"));
        assert!(is_audio_file("concert.mka"));
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

    #[test]
    fn test_normalize_mediainfo_tag_key() {
        assert_eq!(normalize_mediainfo_tag_key("Title"), "track_title");
        assert_eq!(normalize_mediainfo_tag_key("Performer"), "track_artist");
        assert_eq!(normalize_mediainfo_tag_key("Album"), "album_title");
        assert_eq!(
            normalize_mediainfo_tag_key("Album/Performer"),
            "album_artist"
        );
        assert_eq!(
            normalize_mediainfo_tag_key("Track/Position"),
            "track_number"
        );
        assert_eq!(normalize_mediainfo_tag_key("Part/Position"), "disc_number");
        assert_eq!(normalize_mediainfo_tag_key("Genre"), "genre");
        assert_eq!(
            normalize_mediainfo_tag_key("Some Custom Tag"),
            "some_custom_tag"
        );
    }

    #[test]
    fn test_parse_mediainfo_json() {
        let json: serde_json::Value = serde_json::json!({
            "media": {
                "track": [
                    {
                        "@type": "General",
                        "Title": "Test Song",
                        "Performer": "Test Artist",
                        "Album": "Test Album",
                        "Genre": "Rock",
                        "Format": "WMA",
                        "FileSize": "12345",
                    },
                    {
                        "@type": "Audio",
                        "Duration": "240.500",
                        "BitRate": "320000",
                        "SamplingRate": "44100",
                        "BitDepth": "16",
                        "Channels": "2",
                    }
                ]
            }
        });

        let meta = parse_mediainfo_json(&json).unwrap();

        // Check properties
        let props: HashMap<&str, &serde_json::Value> = meta
            .properties
            .iter()
            .map(|(k, v)| (k.as_str(), v))
            .collect();
        assert_eq!(props["audio_duration_ms"], 240500);
        assert_eq!(props["audio_bitrate"], 320);
        assert_eq!(props["audio_sample_rate"], 44100);
        assert_eq!(props["audio_bit_depth"], 16);
        assert_eq!(props["audio_channels"], 2);

        // Check tags (Format/FileSize should be filtered out)
        let tags: HashMap<&str, &serde_json::Value> =
            meta.tags.iter().map(|(k, v)| (k.as_str(), v)).collect();
        assert_eq!(tags["track_title"], "Test Song");
        assert_eq!(tags["track_artist"], "Test Artist");
        assert_eq!(tags["album_title"], "Test Album");
        assert_eq!(tags["genre"], "Rock");
        assert!(!tags.contains_key("format"));
        assert!(!tags.contains_key("file_size"));

        assert!(meta.images.is_empty());
        assert!(meta.cue_sheet.is_none());
    }

    #[test]
    fn test_extension_dispatch() {
        assert!(MEDIAINFO_EXTENSIONS.contains(&"wma"));
        assert!(MEDIAINFO_EXTENSIONS.contains(&"mka"));
        assert!(!MEDIAINFO_EXTENSIONS.contains(&"mp3"));
        assert!(!MEDIAINFO_EXTENSIONS.contains(&"dsf"));
        assert!(!MEDIAINFO_EXTENSIONS.contains(&"flac"));
    }

    #[test]
    fn test_parse_mediainfo_json_no_audio_track() {
        let json = serde_json::json!({
            "media": {
                "track": [
                    {"@type": "General", "Title": "metadata only"}
                ]
            }
        });
        let meta = parse_mediainfo_json(&json).unwrap();
        assert!(meta.properties.is_empty());
        assert_eq!(meta.tags.len(), 1);
        assert_eq!(meta.tags[0].0, "track_title");
    }

    #[test]
    fn test_parse_mediainfo_json_duplicate_keys_deduplicated() {
        // Both Title and Track map to track_title — should deduplicate
        let json = serde_json::json!({
            "media": {
                "track": [
                    {
                        "@type": "General",
                        "Title": "From Title",
                        "Track": "From Track",
                    },
                    {"@type": "Audio", "Duration": "10.0"}
                ]
            }
        });
        let meta = parse_mediainfo_json(&json).unwrap();
        let titles: Vec<&str> = meta
            .tags
            .iter()
            .filter(|(k, _)| k == "track_title")
            .map(|(_, v)| v.as_str().unwrap())
            .collect();
        // Should have exactly one track_title, not two
        assert_eq!(titles.len(), 1);
    }

    #[test]
    fn test_parse_mediainfo_json_unrecognized_fields_filtered() {
        let json = serde_json::json!({
            "media": {
                "track": [
                    {
                        "@type": "General",
                        "Title": "Real Tag",
                        "Compilation": "1",
                        "Format": "WMA",
                        "Encoded_Application": "some encoder",
                        "InternetMediaType": "audio/x-ms-wma",
                    },
                    {"@type": "Audio"}
                ]
            }
        });
        let meta = parse_mediainfo_json(&json).unwrap();
        let tag_keys: Vec<&str> = meta.tags.iter().map(|(k, _)| k.as_str()).collect();
        assert!(tag_keys.contains(&"track_title"));
        // These are not in the allowlist
        assert!(!tag_keys.contains(&"compilation"));
        assert!(!tag_keys.contains(&"format"));
        assert!(!tag_keys.contains(&"encoded_application"));
        assert!(!tag_keys.contains(&"internetmediatype"));
    }

    #[test]
    fn test_normalize_mediainfo_content_type() {
        assert_eq!(normalize_mediainfo_tag_key("ContentType"), "content_type");
    }

    #[test]
    fn test_normalize_mediainfo_lyricist_vs_lyrics() {
        assert_eq!(normalize_mediainfo_tag_key("Lyricist"), "lyricist");
        assert_eq!(normalize_mediainfo_tag_key("Lyrics"), "lyrics");
    }
}

use sqlx::SqlitePool;

#[derive(Debug, sqlx::FromRow, serde::Serialize)]
pub struct Song {
    pub id: i64,
    pub title: String,
    pub artist: String,
    pub album: String,
    pub path: String,
    pub duration: i64,
}

pub async fn init(pool: &SqlitePool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS songs (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            title    TEXT NOT NULL,
            artist   TEXT NOT NULL,
            album    TEXT NOT NULL,
            path     TEXT NOT NULL UNIQUE,
            duration INTEGER NOT NULL
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn upsert_song(pool: &SqlitePool, song: &Song) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO songs (title, artist, album, path, duration)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(path) DO UPDATE SET
             title    = excluded.title,
             artist   = excluded.artist,
             album    = excluded.album,
             duration = excluded.duration",
    )
    .bind(&song.title)
    .bind(&song.artist)
    .bind(&song.album)
    .bind(&song.path)
    .bind(song.duration)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn all_songs(pool: &SqlitePool) -> anyhow::Result<Vec<Song>> {
    let songs = sqlx::query_as::<_, Song>("SELECT * FROM songs ORDER BY artist, album, title")
        .fetch_all(pool)
        .await?;
    Ok(songs)
}

pub async fn find_song(pool: &SqlitePool, id: i64) -> anyhow::Result<Option<Song>> {
    let song = sqlx::query_as::<_, Song>("SELECT * FROM songs WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await?;
    Ok(song)
}

pub async fn find_song_by_path(pool: &SqlitePool, path: &str) -> anyhow::Result<Option<Song>> {
    let song = sqlx::query_as::<_, Song>("SELECT * FROM songs WHERE path = ?")
        .bind(path)
        .fetch_optional(pool)
        .await?;
    Ok(song)
}

pub async fn search_songs(pool: &SqlitePool, q: &str) -> anyhow::Result<Vec<Song>> {
    let pattern = format!("%{}%", q);
    let songs = sqlx::query_as::<_, Song>(
        "SELECT * FROM songs WHERE title LIKE ? OR artist LIKE ? OR album LIKE ?
         ORDER BY artist, album, title",
    )
    .bind(&pattern)
    .bind(&pattern)
    .bind(&pattern)
    .fetch_all(pool)
    .await?;
    Ok(songs)
}

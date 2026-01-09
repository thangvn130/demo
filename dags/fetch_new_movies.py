from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _get_tmdb_api_key() -> str:
    api_key = os.getenv("TMDB_API_KEY") or Variable.get("TMDB_API_KEY", default_var=None)
    if not api_key:
        raise AirflowException("TMDB_API_KEY chưa được cấu hình (Airflow Variable).")
    return api_key


def fetch_genres(**context) -> List[Dict[str, Any]]:
    api_key = _get_tmdb_api_key()
    url = "https://api.themoviedb.org/3/genre/movie/list"
    params = {"api_key": api_key, "language": "vi-VN"}
    resp = requests.get(url, params=params, timeout=30)
    if resp.status_code != 200:
        raise AirflowException(f"TMDB genres lỗi {resp.status_code}: {resp.text}")
    data = resp.json()
    genres = data.get("genres", [])
    if not genres:
        raise AirflowException("TMDB genres trả về rỗng.")
    context["ti"].xcom_push(key="genres", value=genres)
    return genres


def fetch_new_movies(**context) -> List[Dict[str, Any]]:
    api_key = _get_tmdb_api_key()
    url = "https://api.themoviedb.org/3/movie/now_playing"
    params = {"api_key": api_key, "language": "vi-VN", "page": 1}

    all_results: List[Dict[str, Any]] = []
    for page in (1, 2):
        params["page"] = page
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            raise AirflowException(f"TMDB now_playing lỗi {resp.status_code}: {resp.text}")
        data = resp.json()
        results = data.get("results", [])
        all_results.extend(results)

    if not all_results:
        raise AirflowException("TMDB now_playing trả về rỗng.")

    context["ti"].xcom_push(key="movies_raw", value=all_results)
    return all_results


def transform_movies(**context) -> Tuple[List[Dict[str, Any]], List[Tuple[int, int]]]:
    movies_raw = context["ti"].xcom_pull(key="movies_raw", task_ids="fetch_new_movies") or []
    genres = context["ti"].xcom_pull(key="genres", task_ids="fetch_genres") or []

    genre_map = {g["id"]: g["name"] for g in genres}
    movies_clean: List[Dict[str, Any]] = []
    movie_genres: List[Tuple[int, int]] = []

    for mv in movies_raw:
        movie_id = mv.get("id")
        if not movie_id:
            continue
        movies_clean.append(
            {
                "id": movie_id,
                "title": mv.get("title"),
                "overview": mv.get("overview") or "",
                "poster_path": mv.get("poster_path") or "",
                "release_date": mv.get("release_date") or None,
                "vote_average": mv.get("vote_average") or 0,
                "vote_count": mv.get("vote_count") or 0,
                "popularity": mv.get("popularity") or 0,
            }
        )
        for gid in mv.get("genre_ids", []):
            if gid in genre_map:
                movie_genres.append((movie_id, gid))

    context["ti"].xcom_push(key="movies_clean", value=movies_clean)
    context["ti"].xcom_push(key="movie_genres", value=movie_genres)
    return movies_clean, movie_genres


def upsert_movies(**context):
    movies = context["ti"].xcom_pull(key="movies_clean", task_ids="transform_movies") or []
    movie_genres = context["ti"].xcom_pull(key="movie_genres", task_ids="transform_movies") or []
    genres = context["ti"].xcom_pull(key="genres", task_ids="fetch_genres") or []

    if not movies:
        raise AirflowException("Không có dữ liệu phim sau khi transform.")

    hook = PostgresHook(postgres_conn_id="postgres_demo")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS genres (
            id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS new_movies (
            id INT PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            overview TEXT,
            poster_path VARCHAR(500),
            release_date DATE,
            vote_average NUMERIC(3,1),
            vote_count INT,
            popularity NUMERIC(10,2)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS new_movie_genres (
            movie_id INT REFERENCES new_movies(id) ON DELETE CASCADE,
            genre_id INT REFERENCES genres(id) ON DELETE CASCADE,
            PRIMARY KEY (movie_id, genre_id)
        );
        """
    )

    if genres:
        cur.executemany(
            """
            INSERT INTO genres (id, name)
            VALUES (%(id)s, %(name)s)
            ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;
            """,
            genres,
        )

    cur.executemany(
        """
        INSERT INTO new_movies (id, title, overview, poster_path, release_date,
                                vote_average, vote_count, popularity)
        VALUES (%(id)s, %(title)s, %(overview)s, %(poster_path)s, %(release_date)s,
                %(vote_average)s, %(vote_count)s, %(popularity)s)
        ON CONFLICT (id) DO UPDATE SET
            title = EXCLUDED.title,
            overview = EXCLUDED.overview,
            poster_path = EXCLUDED.poster_path,
            release_date = EXCLUDED.release_date,
            vote_average = EXCLUDED.vote_average,
            vote_count = EXCLUDED.vote_count,
            popularity = EXCLUDED.popularity;
        """,
        movies,
    )

    if movie_genres:
        movie_ids = list({m[0] for m in movie_genres})
        cur.execute(
            "DELETE FROM new_movie_genres WHERE movie_id = ANY(%s);",
            (movie_ids,),
        )
        cur.executemany(
            "INSERT INTO new_movie_genres (movie_id, genre_id) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
            movie_genres,
        )

    conn.commit()
    cur.close()
    conn.close()

    context["ti"].xcom_push(key="upsert_count", value=len(movies))
    return len(movies)


def log_summary(**context):
    count = context["ti"].xcom_pull(key="upsert_count", task_ids="upsert_movies") or 0
    print(f"✅ Đã upsert {count} phim vào PostgreSQL.")


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fetch_new_movies",
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    default_args=default_args,
    tags=["movies", "tmdb"],
) as dag:

    t_genres = PythonOperator(
        task_id="fetch_genres",
        python_callable=fetch_genres,
    )

    t_fetch = PythonOperator(
        task_id="fetch_new_movies",
        python_callable=fetch_new_movies,
    )

    t_transform = PythonOperator(
        task_id="transform_movies",
        python_callable=transform_movies,
    )

    t_upsert = PythonOperator(
        task_id="upsert_movies",
        python_callable=upsert_movies,
    )

    t_log = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary,
    )

    t_genres >> t_fetch >> t_transform >> t_upsert >> t_log

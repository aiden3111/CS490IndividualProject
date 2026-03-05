import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";

const API = "http://127.0.0.1:5000/api";

export default function ActorDetails() {
  const { id } = useParams();

  const [actor, setActor] = useState(null);
  const [films, setFilms] = useState([]);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    async function load() {
      try {
        setLoading(true);
        setError("");

        const [aRes, fRes] = await Promise.all([
          fetch(`${API}/actors/${id}`),
          fetch(`${API}/actors/${id}/top-films`),
        ]);

        const aJson = await aRes.json();
        if (!aRes.ok) throw new Error(aJson.error || "Failed to load actor");

        const fJson = await fRes.json();
        if (!fRes.ok) throw new Error(fJson.error || "Failed to load actor films");

        setActor(aJson);
        setFilms(fJson);
      } catch (e) {
        setError(e.message);
      } finally {
        setLoading(false);
      }
    }

    load();
  }, [id]);

  return (
    <div style={{ fontFamily: "Arial", padding: 20 }}>
      <p>
        <Link to="/">Back to Landing</Link>
      </p>

      {loading && <p>Loading...</p>}
      {error && <p style={{ color: "red" }}>Error: {error}</p>}

      {!loading && !error && actor && (
        <>
          <h1>
            {actor.first_name} {actor.last_name}
          </h1>

          <h2>Top 5 Rented Films (for this actor)</h2>

          {films.length === 0 ? (
            <p>No films found.</p>
          ) : (
            <ul>
              {films.map((f) => (
                <li key={f.film_id}>
                  <Link to={`/films/${f.film_id}`}>
                    <b>{f.title}</b>
                  </Link>{" "}
                  — ({f.rental_count} rentals)
                </li>
              ))}
            </ul>
          )}
        </>
      )}
    </div>
  );
}

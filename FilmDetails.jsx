import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";

const API = "http://127.0.0.1:5000/api";

export default function FilmDetails() {
    const { id } = useParams();
    const [film, setFilm] = useState(null);
    const [error, setError] = useState("");
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        fetch(`${API}/films/${id}`)
            .then(async (res) => {
                const data = await res.json();
                if (!res.ok) throw new Error(data.error || "Failed to load film");
                return data;
            })
            .then((data) => {
                setFilm(data);
                setError("");
            })
            .catch((e) => setError(e.message))
            .finally(() => setLoading(false));
        }, [id]);

    return (
        <div style={{ fontFamily: "Arial", padding: 20}}>
            <p><Link to="/">Back to Top Films</Link></p>

            {loading && <p>Loading..</p>}
            {error && <p style={{ color: "red" }}>Error: {error}</p>}

            {film && (
                <>
                    <h1>{film.title}</h1>
                    <p>{film.description}</p>
                    
                    <ul>
                        <li><b>Category:</b> {film.category}</li>
                        <li><b>Release Year:</b> {film.release_year}</li>
                        <li><b>Length:</b> {film.length}</li>
                        <li><b>Rating:</b> {film.rating}</li>
                        <li><b>Rental Rate:</b> {film.rental_rate}</li>
                        </ul>
                    </>
            )}
        </div>
    );
}
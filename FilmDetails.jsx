import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";

const API = "http://127.0.0.1:5000/api";

export default function FilmDetails() {
    const { id } = useParams();
    const [film, setFilm] = useState(null);
    const [error, setError] = useState("");
    const [loading, setLoading] = useState(true);

    const [customerId, setCustomerId] = useState("");
    const [rentMsg, setRentMsg] = useState("");
    const [rentError, setRentError] = useState("");
    const [rentLoading, setRentLoading] = useState(false);

    useEffect(() => {
        setRentMsg("");
        setRentError("");
        setCustomerId("");
        
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

    async function rentFilm() {
        setRentMsg("");
        setRentError("");

        if (!customerId.trim() || isNaN(Number(customerId))) {
            setRentError("Enter a valid numeric customer_id");
            return;
        }

        try {
            setRentLoading(true);

            const res = await fetch(`${API}/rentals`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                customer_id: Number(customerId),
                film_id: Number(id),
                staff_id: 1,
            }),
            });

            const json = await res.json();
            if (!res.ok) throw new Error(json.error || "Rent failed");

            setRentMsg(`Success! rental_id=${json.rental_id} (inventory_id=${json.inventory_id})`);
            setCustomerId("");
        } catch (e) {
            setRentError(e.message);
        } finally {
            setRentLoading(false);
        }
    }

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

                    <hr style={{ margin: "20px 0" }} />

                    <h2>Rent this film</h2>

                    <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                    <input
                        value={customerId}
                        onChange={(e) => setCustomerId(e.target.value)}
                        placeholder="Customer ID"
                        style={{ padding: 8, width: 160 }}
                    />
                    <button
                        onClick={rentFilm}
                        disabled={rentLoading}
                        style={{ padding: "8px 12px" }}
                    >
                        {rentLoading ? "Renting..." : "Rent"}
                    </button>
                    </div>

                    {rentMsg && <p style={{ color: "green" }}>{rentMsg}</p>}
                    {rentError && <p style={{ color: "red" }}>{rentError}</p>}
                    </>
            )}
        </div>
    );
}

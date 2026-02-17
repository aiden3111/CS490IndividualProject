import { useEffect, useState } from "react";
import { Routes, Route, Link } from "react-router-dom";
import FilmDetails from "./pages/FilmDetails.jsx";
import Customers from "./pages/Customers.jsx";

const API = "http://127.0.0.1:5000/api";

function Landing() {
  const [films, setFilms] = useState([]);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`${API}/top-films`)
      .then(async (res) => {
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Loading failed");
        return data;
      })
      .then((data) => {
        setFilms(data);
        setError("");
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  return (
    <div style={{ fontFamily: "Arial", padding: 20}}>
      <h1>Top 5 Rented Films</h1>

      <p><Link to="/customers">Go to Customers</Link></p>


      {loading && <p>Loading..</p>}
      {error && <p style={{ color: "red" }}>Error: {error}</p>}

      <ul>
        {films.map((f) => (
          <li key={f.film_id}>
            <Link to={`/films/${f.film_id}`}>
              <b>{f.title}</b>
            </Link>
            — {f.category} ({f.rental_count} rentals)
          </li>
        ))}
      </ul>
    </div>
  );

}

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<Landing />} />
      <Route path="/films/:id" element={<FilmDetails />} />
      <Route path="/customers" element={<Customers />} />
    </Routes>
  )
}
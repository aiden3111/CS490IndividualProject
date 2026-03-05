import { useEffect, useState } from "react";
import { Routes, Route, Link } from "react-router-dom";
import FilmDetails from "./pages/FilmDetails.jsx";
import Customers from "./pages/Customers.jsx";
import ActorDetails from "./pages/ActorDetails.jsx";
import CustomerDetails from "./pages/CustomerDetails.jsx"; 


const API = "http://127.0.0.1:5000/api";

function Landing() {
  const [films, setFilms] = useState([]);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [actors, setActors] = useState([]);
  const [actorError, setActorError] = useState("");
  const [actorLoading, setActorLoading] = useState(true);
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [searchError, setSearchError] = useState("");

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

  useEffect(() => {
    fetch(`${API}/top-actors`)
      .then(async (res) => {
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Loading actors failed");
        return data;
      })
      .then((data) => {
        setActors(data);
        setActorError("");
      })
      .catch((e) => setActorError(e.message))
      .finally(() => setActorLoading(false));
  }, []);

  async function searchFilms() {
    if (!searchQuery.trim()) return;

    try {
      setSearchLoading(true);
      setSearchError("");

      const res = await fetch(`${API}/films/search?q=${encodeURIComponent(searchQuery)}`);
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || "Search failed");

      setSearchResults(json);
    } catch (e) {
      setSearchError(e.message);
    } finally {
      setSearchLoading(false);
    }
  }

  return (
    <div style={{ fontFamily: "Arial", padding: 20}}>
      <h2>Search Films</h2>

<div style={{ marginBottom: 15 }}>
  <input
    value={searchQuery}
    onChange={(e) => setSearchQuery(e.target.value)}
    placeholder="Search by title, category, or actor"
    style={{ padding: 8, width: 320, marginRight: 8 }}
  />
  <button onClick={searchFilms} style={{ padding: "8px 12px" }}>
    Search
  </button>
</div>

{searchLoading && <p>Searching...</p>}
{searchError && <p style={{ color: "red" }}>{searchError}</p>}

{searchResults.length > 0 && (
  <>
    <h3>Search Results</h3>
      <ul>
        {searchResults.map((f) => (
          <li key={f.film_id}>
            <Link to={`/films/${f.film_id}`}>
              <b>{f.title}</b>
            </Link>{" "}
            — {f.category}
          </li>
        ))}
      </ul>
    </>
  )}
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

      <hr style={{ margin: "20px 0" }} />

    <h1>Top 5 Actors</h1>

    {actorLoading && <p>Loading..</p>}
    {actorError && <p style={{ color: "red" }}>Error: {actorError}</p>}

    <ul>
      {actors.map((a) => (
        <li key={a.actor_id}>
          <Link to={`/actors/${a.actor_id}`}>
            <b>{a.first_name} {a.last_name}</b>
          </Link>
          — ({a.rental_count} rentals)
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
      <Route path="/customers/:id" element={<CustomerDetails />} />
      <Route path="/actors/:id" element={<ActorDetails />} />
    </Routes>
  )
}

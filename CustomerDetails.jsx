import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";

const API = "http://127.0.0.1:5000/api";

export default function CustomerDetails() {
  const { id } = useParams();

  const [customer, setCustomer] = useState(null);
  const [rentals, setRentals] = useState([]);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const [returnMsg, setReturnMsg] = useState("");
  const [returnError, setReturnError] = useState("");
  const [returnLoadingId, setReturnLoadingId] = useState(null);

  async function loadAll() {
    try {
      setLoading(true);
      setError("");

      const [cRes, rRes] = await Promise.all([
        fetch(`${API}/customers/${id}`),
        fetch(`${API}/customers/${id}/rentals`),
      ]);

      const cJson = await cRes.json();
      if (!cRes.ok) throw new Error(cJson.error || "Failed to load customer");

      const rJson = await rRes.json();
      if (!rRes.ok) throw new Error(rJson.error || "Failed to load rentals");

      setCustomer(cJson);
      setRentals(rJson);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadAll();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id]);

  async function markReturned(rental_id) {
    setReturnMsg("");
    setReturnError("");

    try {
      setReturnLoadingId(rental_id);

      const res = await fetch(`${API}/rentals/${rental_id}/return`, {
        method: "PUT",
      });

      const json = await res.json();
      if (!res.ok) throw new Error(json.error || "Failed to mark returned");

      setReturnMsg(`Returned rental_id=${rental_id}`);
      // reload rentals so UI updates
      await loadAll();
    } catch (e) {
      setReturnError(e.message);
    } finally {
      setReturnLoadingId(null);
    }
  }

  return (
    <div style={{ fontFamily: "Arial", padding: 20 }}>
      <p>
        <Link to="/customers">Back to Customers</Link> |{" "}
        <Link to="/">Back to Landing</Link>
      </p>

      {loading && <p>Loading...</p>}
      {error && <p style={{ color: "red" }}>Error: {error}</p>}

      {!loading && !error && customer && (
        <>
          <h1>
            Customer #{customer.customer_id}: {customer.first_name}{" "}
            {customer.last_name}
          </h1>

          <ul>
            <li>
              <b>Email:</b> {customer.email || "(none)"}
            </li>
            <li>
              <b>Store:</b> {customer.store_id}
            </li>
            <li>
              <b>Address ID:</b> {customer.address_id}
            </li>
            <li>
              <b>Active:</b> {customer.active}
            </li>
            <li>
              <b>Create Date:</b> {String(customer.create_date)}
            </li>
          </ul>

          <hr style={{ margin: "20px 0" }} />

          <h2>Rental History</h2>

          {returnMsg && <p style={{ color: "green" }}>{returnMsg}</p>}
          {returnError && <p style={{ color: "red" }}>{returnError}</p>}

          {rentals.length === 0 ? (
            <p>No rentals found.</p>
          ) : (
            <table
              border="1"
              cellPadding="8"
              style={{ borderCollapse: "collapse" }}
            >
              <thead>
                <tr>
                  <th>Rental ID</th>
                  <th>Film</th>
                  <th>Rented</th>
                  <th>Returned</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {rentals.map((r) => {
                  const isReturned = r.return_date !== null;
                  return (
                    <tr key={r.rental_id}>
                      <td>{r.rental_id}</td>
                      <td>
                        <Link to={`/films/${r.film_id}`}>{r.title}</Link>
                      </td>
                      <td>{String(r.rental_date)}</td>
                      <td>{isReturned ? String(r.return_date) : "NOT RETURNED"}</td>
                      <td>
                        <button
                          disabled={isReturned || returnLoadingId === r.rental_id}
                          onClick={() => markReturned(r.rental_id)}
                        >
                          {isReturned
                            ? "Returned"
                            : returnLoadingId === r.rental_id
                            ? "Returning..."
                            : "Mark Returned"}
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </>
      )}
    </div>
  );
}

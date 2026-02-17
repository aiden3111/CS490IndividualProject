import { act, use, useEffect, useState } from "react";

const API = "http://127.0.0.1:5000/api";

export default function Customers() {
    const [q, setQ] = useState("");
    const [page, setPage] = useState(1);
    const limit = 10;

    const [data, setData] = useState(null);
    const [error, setError] = useState("");
    const [loading, setLoading] = useState(true);

    const [addForm, setAddForm] = useState({
        store_id: 1,
        first_name: "",
        last_name: "",
        email: "",
        address_id: 1,
        active: 1
    });
    const [addMsg, setAddMsg] = useState("");

    const [editingId, setEditingId] = useState(null);
    const [editForm, setEditForm] = useState({
        first_name: "",
        last_name: "",
        email: "",
        active: 1,
        store_id: 1,
        address_id: 1,
    });

    function load(p = page, query = q) {
        setLoading(true);
        setError("");

        const url = `${API}/customers?q=${encodeURIComponent(query)}&page=${p}&limit=${limit}`;

        fetch(url)
            .then(async (res) => {
                const json = await res.json();
                if (!res.ok) throw new Error(json.error || "Failed to fetch customers");
                return json;
            })
            .then((json) => setData(json))
            .catch((e) => setError(e.message))
            .finally(() => setLoading(false));
    }

    useEffect(() => {
        load(1, "");
    }, []);

    const customers = data?.customers ?? [];
    const total = data?.total ?? 0;
    const totalPages = Math.max(1, Math.ceil(total / limit));

    async function addCustomer(e) {
        e.preventDefault();
        setAddMsg("");
        setError("");

        try {
            const res = await fetch(`${API}/customers`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    store_id: Number(addForm.store_id),
                    first_name: addForm.first_name,
                    last_name: addForm.last_name,
                    email: addForm.email,
                    address_id: Number(addForm.address_id),
                    active: Number(addForm.active),
                }),
            });

            const json = await res.json();
            if (!res.ok) throw new Error(json.error || "Failed to add customer");

            setAddMsg(`Added customer_id ${json.customer_id}`);
            setAddForm((f) => ({ ...f, first_name: "", last_name: "", email: "" }));

            // reload current page
            load(page, q);
        } catch (e2) {
            setError(e2.message);
        }
    }
    
    function startEdit(c) {
        setEditingId(c.customer_id);
        setEditForm({
            first_name: c.first_name ?? "",
            last_name: c.last_name ?? "",
            email: c.email ?? "",
            active: c.active ?? 1,
            store_id: c.store_id ?? 1,
            address_id: c.address_id ?? 1,
        });
    }

    function cancelEdit() {
        setEditingId(null);
    }

    async function saveEdit(customer_id) {
        setError("");

        try {
            const res = await fetch(`${API}/customers/${customer_id}`, {
                method: "PUT",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    first_name: editForm.first_name,
                    last_name: editForm.last_name,
                    email: editForm.email,
                    active: Number(editForm.active),
                    store_id: Number(editForm.store_id),
                    address_id: Number(editForm.address_id),
                }),
            });

            const json = await res.json();
            if (!res.ok) throw new Error(json.error || "Failed to update customer");

            setEditingId(null);
            load(page, q);
        } catch (e) {
            setError(e.message);
        }
    }

    async function deleteCustomer(customer_id) {
        setError("");

        // optional confirm
        const ok = window.confirm(`Deactive customer ${customer_id}? This cannot be undone.`);
        if (!ok) return;

        try {
            const res = await fetch(`${API}/customers/${customer_id}`, {
                method: "DELETE",
            });
            const json = await res.json();
            if (!res.ok) throw new Error(json.error || "Failed to delete customer");

            load(page, q);
        } catch (e) {
            setError(e.message);
        }
    }

    return (
        <div style={{ fontFamily: "Arial", padding: 20}}>
            <h1>Customers</h1>

            <div style={{ marginBottom: 12 }}>
                <input
                    value={q}
                    placeholder = "Search by id, first name, last name"
                    onChange={(e) => setQ(e.target.value)}
                    style={{ padding: 8, width: 320, marginRight: 8 }}
                />
                <button
                    onClick={() => {
                        setPage(1);
                        load(1,q);
                    }}
                    style ={{ padding: "8px 12px" }}
                >
                    Search
                </button>
        </div>

        {/* Add Customer */}
        <form onSubmit={addCustomer} style={{ marginBottom: 18}}>
            <h3 style={{ margin: "10px 0" }}>Add Customer</h3>

            <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                <input
                    placeholder="First name"
                    value={addForm.first_name}
                    onChange={(e) => setAddForm((f) => ({ ...f, first_name: e.target.value }))}
                    style={{ padding: 8 }}
                    required
                />
                <input
                    placeholder="Last name"
                    value={addForm.last_name}
                    onChange={(e) => setAddForm((f) => ({ ...f, last_name: e.target.value }))}
                    style={{ padding: 8 }}
                    required
                />
                <input
                    placeholder="Email (optional)"
                    value={addForm.email}
                    onChange={(e) => setAddForm((f) => ({ ...f, email: e.target.value }))}
                    style={{ padding: 8}}
                    required
                />
                <input
                    placeholder="Store ID"
                    type="number"
                    value={addForm.store_id}
                    onChange={(e) => setAddForm((f) => ({ ...f, store_id: e.target.value }))}
                    style={{ padding: 8, width: 110 }}
                    required
                />
                <input
                    placeholder="Address ID"
                    type="number"
                    value={addForm.address_id}
                    onChange={(e) => setAddForm((f) => ({ ...f, address_id: e.target.value }))}
                    style={{ padding: 8, width: 110 }}
                    required
                />
                <select
                    value={addForm.active}
                    onChange={(e) => setAddForm((f) => ({ ...f, active: e.target.value }))}
                    style={{ padding: 8}}
                >
                    <option value={1}>Active</option>
                    <option value={0}>Inactive</option>
                </select>

                <button type="submit" style={{ padding: "8px 12px" }}>
                    Add
                </button>
            </div>

            {addMsg && <p style={{ color: "green" }}>{addMsg}</p>}
            </form>



        {loading && <p>Loading..</p>}
        {error && <p style={{ color: "red" }}>{error}</p>}

        {!loading && !error && (
            <>
                <p>
                    Showing page {page} of {totalPages} (total: {total})
                </p>
                
                <table border="1" cellPadding="8" style={{ borderCollapse: "collapse" }}>
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>First</th>
                            <th>Last</th>
                            <th>Email</th>
                            <th>Store</th>
                            <th>Address</th>
                            <th>Active</th>
                            <th>Actions</th>
                        </tr>
                    </thead>

                    <tbody>
                        {customers.map((c) => {
                const isEditing = editingId === c.customer_id;

                return (
                  <tr key={c.customer_id}>
                    <td>{c.customer_id}</td>

                    <td>
                      {isEditing ? (
                        <input
                          value={editForm.first_name}
                          onChange={(e) =>
                            setEditForm((f) => ({ ...f, first_name: e.target.value }))
                          }
                        />
                      ) : (
                        c.first_name
                      )}
                    </td>

                    <td>
                      {isEditing ? (
                        <input
                          value={editForm.last_name}
                          onChange={(e) =>
                            setEditForm((f) => ({ ...f, last_name: e.target.value }))
                          }
                        />
                      ) : (
                        c.last_name
                      )}
                    </td>

                    <td>
                      {isEditing ? (
                        <input
                          value={editForm.email}
                          onChange={(e) => setEditForm((f) => ({ ...f, email: e.target.value }))}
                        />
                      ) : (
                        c.email
                      )}
                    </td>

                    <td>
                      {isEditing ? (
                        <input
                          type="number"
                          value={editForm.store_id}
                          onChange={(e) =>
                            setEditForm((f) => ({ ...f, store_id: e.target.value }))
                          }
                          style={{ width: 80 }}
                        />
                      ) : (
                        c.store_id
                      )}
                    </td>

                    <td>
                      {isEditing ? (
                        <input
                          type="number"
                          value={editForm.address_id}
                          onChange={(e) =>
                            setEditForm((f) => ({ ...f, address_id: e.target.value }))
                          }
                          style={{ width: 90 }}
                        />
                      ) : (
                        c.address_id
                      )}
                    </td>

                    <td>
                      {isEditing ? (
                        <select
                          value={editForm.active}
                          onChange={(e) => setEditForm((f) => ({ ...f, active: e.target.value }))}
                        >
                          <option value={1}>1</option>
                          <option value={0}>0</option>
                        </select>
                      ) : (
                        c.active
                      )}
                    </td>

                    <td>
                      {!isEditing ? (
                        <>
                          <button onClick={() => startEdit(c)} style={{ marginRight: 8 }}>
                            Edit
                          </button>
                          <button onClick={() => deleteCustomer(c.customer_id)}>Delete</button>
                        </>
                      ) : (
                        <>
                          <button
                            onClick={() => saveEdit(c.customer_id)}
                            style={{ marginRight: 8 }}
                          >
                            Save
                          </button>
                          <button onClick={cancelEdit}>Cancel</button>
                        </>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>

          <div style={{ marginTop: 12 }}>
            <button
              disabled={page <= 1}
              onClick={() => {
                const p = page - 1;
                setPage(p);
                load(p, q);
              }}
              style={{ padding: "8px 12px", marginRight: 8 }}
            >
              Prev
            </button>

            <button
              disabled={page >= totalPages}
              onClick={() => {
                const p = page + 1;
                setPage(p);
                load(p, q);
              }}
              style={{ padding: "8px 12px" }}
            >
              Next
            </button>
          </div>
        </>
      )}
    </div>
  );
}
from flask import Flask, jsonify, request
from flask_cors import CORS
import mysql.connector
from mysql.connector import Error

app = Flask(__name__)
CORS(app)

def get_db():
    return mysql.connector.connect(
         host ="localhost",
         user = "root",
         password = "NTfDBc91925!",
         database = "sakila"
    )

# top 5 rented movies
@app.route("/api/top-films")
def top_films():
    db = get_db()
    cursor = db.cursor(dictionary=True)

    #SQL Query:

    cursor.execute("""
        SELECT f.film_id, f.title, c.name AS category, COUNT(r.rental_id) AS rental_count
        FROM rental r
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN film f ON i.film_id = f.film_id
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        GROUP BY f.film_id, f.title, c.name
        ORDER BY rental_count DESC
        LIMIT 5
    """)

    rows = cursor.fetchall()
    db.close()
    return jsonify(rows)

# film details
@app.route("/api/films/<int:film_id>")
def film_details(film_id):
    db = get_db()
    cursor = db.cursor(dictionary=True)
    cursor.execute("""
        SELECT f.film_id, f.title, f.description, f.release_year, f.rental_rate, f.length, f.rating, c.name AS category
        FROM film f
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        WHERE f.film_id = %s
    """, (film_id,))
    row = cursor.fetchone()
    db.close()

    if not row:
        return jsonify({"error": "Film not found"}), 404
    
    return jsonify(row)

# search for films
@app.route("/api/films/search")
def search_films():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "Search is empty"}), 400
    db = get_db()
    cursor = db.cursor(dictionary=True)
    cursor.execute("""
        SELECT DISTINCT f.film_id, f.title, c.name AS category
        FROM film f
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        LEFT JOIN film_actor fa ON f.film_id = fa.film_id
        LEFT JOIN actor a ON fa.actor_id = a.actor_id
        WHERE f.title LIKE %s
            OR c.name LIKE %s
            OR CONCAT(a.first_name, ' ', a.last_name) LIKE %s
        LIMIT 50
    """, (f"%{q}%", f"%{q}%", f"%{q}%"))
    rows = cursor.fetchall()
    db.close()
    return jsonify(rows)


#customer list and search
@app.route("/api/customers")
def customers():
    q = request.args.get("q", "").strip()
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 10))

    if page < 1: page = 1
    if limit < 1: limit = 10
    if limit > 100: limit = 100

    offset = (page - 1) * limit

    db = get_db()
    cursor = db.cursor(dictionary=True)

    where_sql = "WHERE active = 1"
    params = []

    if q:
        if q.isdigit():
            where_sql += "AND (customer_id = %s OR first_name LIKE %s OR last_name LIKE %s)"
            params = [int(q), f"%{q}%", f"%{q}%"]
        else:
            where_sql += " AND (first_name LIKE %s OR last_name LIKE %s)"
            params = [f"%{q}%", f"%{q}%"]

    # total count
    cursor.execute(f"""
        SELECT COUNT(*) AS total
        FROM customer
        {where_sql}
    """, tuple(params))
    total = cursor.fetchone()["total"]

    # page of customers
    cursor.execute(f"""
        SELECT customer_id, first_name, last_name, email, store_id, address_id, active
        FROM customer
        {where_sql}
        ORDER BY customer_id
        LIMIT %s OFFSET %s
    """, tuple(params + [limit, offset]))
    rows = cursor.fetchall()

    db.close()

    return jsonify({
        "page": page,
        "limit": limit,
        "total": total,
        "customers": rows
    })

@app.route("/api/customers", methods=["POST"])
def add_customer():
    data = request.get_json(force=True) or {}

    required = ["store_id", "first_name", "last_name", "address_id"]
    for k in required:
        if k not in data or str(data[k]).strip() == "":
            return jsonify({"error": f"Missing field: {k}"}), 400
        
    try:
        store_id = int(data["store_id"])
        address_id = int(data["address_id"])
        active = int(data.get("active", 1))
    except (ValueError, TypeError):
        return jsonify({"error": "store_id, address_id, and active must be numbers"}), 400

    first_name = str(data["first_name"]).strip()
    last_name = str(data["last_name"]).strip()
    email = (data.get("email", "") or "").strip() or None

    db = get_db()
    cursor = db.cursor()
    try:
        cursor.execute("""
            INSERT INTO customer (store_id, first_name, last_name, email, address_id, active, create_date)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
        """, (store_id, first_name, last_name, email, address_id, active))

        db.commit()
        return jsonify({"customer_id": cursor.lastrowid}), 201
    except Error as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        db.close()

@app.route("/api/customers/<int:customer_id>", methods=["PUT"])
def update_customer(customer_id):
    data = request.get_json(force=True)

    allowed = ["first_name", "last_name", "email", "active", "store_id", "address_id"]
    updates = []
    params = []

    for k in allowed:
        if k in data:
            updates.append(f"{k} = %s")
            if k in ["active", "store_id", "address_id"]:
                params.append(int(data[k]))
            else:
                val = data[k]
                params.append(val.strip() if isinstance(val, str) else val)
    if not updates:
        return jsonify({"error": "No fields to update"}), 400
    
    db = get_db()
    cursor = db.cursor()

    sql = f"UPDATE customer SET {', '.join(updates)} WHERE customer_id = %s"
    params.append(customer_id)

    cursor.execute(sql, tuple(params))
    db.commit()

    if cursor.rowcount == 0:
        db.close()
        return jsonify({"error": "Customer not found"}), 404
    
    db.close()
    return jsonify({"ok": True})

@app.route("/api/customers/<int:customer_id>", methods=["DELETE"])
def delete_customer(customer_id):
    db = get_db()
    cursor = db.cursor(dictionary=True)

    # check if customer exists
    cursor.execute("SELECT customer_id, active FROM customer WHERE customer_id = %s", (customer_id,))
    row = cursor.fetchone()

    if not row:
        db.close()
        return jsonify({"error": "Customer not found"}), 404
    

    #soft delete

    cursor.execute("""
        UPDATE customer
        SET active = 0
        WHERE customer_id = %s
    """, (customer_id,))
    db.commit()
    db.close()
    return jsonify({"ok": True, "already_inactive": row["active"] == 0})

if __name__ == "__main__":
    app.run(debug=True)
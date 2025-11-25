# The Joy of Painting ETL & API Project

## Overview
This project implements a complete ETL (Extract, Transform, Load) pipeline and REST API for **The Joy of Painting** dataset.  
The goal is to merge, clean, and centralize data from multiple inconsistent sources, then provide an API that allows users to filter episodes by:

- Month of original broadcast  
- Subject matter  
- Color palette  

The final application supports multi‑filter queries and AND/OR filter logic.

---

## 📁 Project Structure
```
atlas-the-joy-of-painting-api/
│
├── README.md
├── database/
│   ├── schema.sql
│   ├── uml-diagram.png
│
├── etl/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── data/
│       ├── bob_ross_episodes.csv
│       ├── bob_ross_colors.csv
│       ├── bob_ross_color_details.csv
│
├── api/
│   ├── server.py
│   ├── routes/
│       ├── episodes.py
│
└── utils/
    ├── db.py
```

---

## 🧩 1. Database Design
This project uses an SQL database (PostgreSQL recommended).  
Your schema includes:

### **Tables**
- `episodes`
- `subjects`
- `colors`
- `episode_subjects` (junction)
- `episode_colors` (junction)

A UML diagram is included in `database/uml-diagram.png`.

---

## 🛠 2. ETL Pipeline
The ETL process loads raw CSV data, standardizes it, resolves inconsistencies, and inserts it into the database.

### Scripts:
- `extract.py` – Reads CSV files  
- `transform.py` – Cleans fields, normalizes lists, validates colors  
- `load.py` – Inserts data into SQL tables  

Run ETL:

```
python3 etl/extract.py
python3 etl/transform.py
python3 etl/load.py
```

---

## 🌐 3. REST API
Built using **Python + Flask**, the API exposes endpoints for filtering episodes.

### Example endpoint:
```
GET /episodes?month=January&colors=Alizarin Crimson,Sap Green&subjects=tree,mountain&mode=and
```

### Query parameters
| Parameter | Description |
|----------|-------------|
| `month` | Filter by month of original broadcast |
| `colors` | Comma‑separated list of colors |
| `subjects` | Comma‑separated list of subjects |
| `mode` | `and` (intersection) or `or` (union) |

---

## ▶ Running the API
```
cd api
python3 server.py
```

API returns JSON with matching episodes.

---

## 🧪 Testing
You may use **Postman**, **cURL**, or **browser query params** to test your API.

---

## 👤 Author Ahmad Nawabi
Created for the Holberton / Atlas School ETL project.

---

## 📌 Notes
This repo does **not** include original CSV files from Bob Ross datasets due to licensing.  
Place them under `etl/data/` before running ETL.

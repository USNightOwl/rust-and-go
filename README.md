# Programming Handbooks: Rust & Go

A collection of Vietnamese-language study handbooks for **Rust** and **Go**. Each one covers the language fundamentals, data structures and algorithms, and backend development. The handbooks are published as a static website on **GitHub Pages**.

## Live site

When you push to `main`, GitHub Actions deploys the site automatically. The landing page (`index.html`) links to each handbook:

| Handbook | Topics |
|---|---|
| [Sổ tay Rust All-in-One](r/Sổ%20tay%20Rust%20All-in-One.html) | Language fundamentals, data structures & algorithms, backend with Axum + SQLx |
| [Sổ tay học Go & Gin](g/So_tay_hoc_Go_Gin.html) | Go fundamentals, data structures & algorithms, backend with Gin |

## Repository structure

```
.
├── index.html                  # Landing page linking to both handbooks
├── .github/workflows/
│   └── deploy.yml              # GitHub Pages deployment workflow
├── r/                          # Rust materials
│   ├── Sổ tay Rust All-in-One.html
│   ├── Rust_P1_Nen_tang_ngon_ngu.docx     # Part 1: Language fundamentals
│   ├── Rust_P2_CTDL_Giai_thuat.docx       # Part 2: Data structures & algorithms
│   ├── Rust_P3_Backend_Axum_SQLx.docx     # Part 3: Backend with Axum + SQLx
│   └── taskapi_du_an_mau/taskapi/         # Sample project (see below)
└── g/                          # Go materials
    ├── So_tay_hoc_Go_Gin.html
    ├── Phan1_Golang_Nen_Tang.docx         # Part 1: Go fundamentals
    ├── Phan2_CTDL_Giai_Thuat_Go.docx      # Part 2: Data structures & algorithms
    └── Phan3_Backend_Go_Gin.docx          # Part 3: Backend with Gin
```

## Sample project: `taskapi` (Rust)

`r/taskapi_du_an_mau/taskapi` is a reference task-management REST API that goes with the Rust handbook. It shows a production-style backend layout:

- **Web framework:** Axum 0.8, with Tower / tower-http middleware (tracing, CORS, timeouts, request IDs)
- **Database:** PostgreSQL via SQLx, with migrations
- **Auth:** JWT (`jsonwebtoken`) and Argon2 password hashing
- **API docs:** OpenAPI via `utoipa`, served with Swagger UI
- **Validation & errors:** `validator`, `thiserror`, `anyhow`
- **Observability:** `tracing` with structured JSON logs
- **Deployment:** Dockerfile, docker-compose, Kubernetes manifests, an nginx config, and a systemd unit
- **Layered architecture:** `routes` → `services` → `repository` → `models`

To run it locally (Rust 1.88+, Docker):

```bash
cd r/taskapi_du_an_mau/taskapi
cp .env.example .env
docker compose up -d     # start PostgreSQL (and the app, if configured)
cargo run
cargo test
```

## Deployment

The workflow at `.github/workflows/deploy.yml`:

1. Runs on every push to `main`, or manually from the **Actions** tab.
2. Copies `index.html`, `r/` and `g/` into a `_site` folder.
3. Uploads `_site` and deploys it to GitHub Pages.

**One-time setup:** go to **Settings → Pages → Build and deployment → Source** and select **GitHub Actions**.

## Viewing locally

The handbooks are self-contained HTML files. Open `index.html` directly in a browser, or serve the folder:

```bash
python -m http.server 8000
# then visit http://localhost:8000
```

## Language

The handbook content is written in **Vietnamese**.

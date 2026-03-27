## ⚠️ Disclaimer – Environment Requirement

The scripts in this project are designed to run in **GitHub Codespaces** using the
**default Ubuntu-based Codespaces image**.

They assume:
- Ubuntu-based OS (default GitHub Codespaces image)
- `sudo` access
- `apt-get` package management
- System-managed **PostgreSQL**
- Internet access for installing system and Python dependencies

> ⚠️ Running these scripts outside GitHub Codespaces (for example: local machines,
> WSL, macOS, Windows, restricted CI runners, or minimal Docker images) may fail or
> require manual changes.

---

## ✅ Supported Environments

- **GitHub Codespaces (recommended)**
- **Default GitHub Codespaces Ubuntu image**
- Internet access for installing system and Python dependencies

---

## 🧱 Technologies Used

- **PostgreSQL** – relational database
- **dbt* - Data Build Tool

---

## 🚀 Usage (GitHub Codespaces)

1. Add this repository (or copy the source code) or into your **GitHub Codespace** project.
2. Open the Codespace using the default Ubuntu-based image
3. Run the desired demo shell scripts that are included in the source code

Each demo script is **self-contained** in its own folder and will:
- Install required system dependencies 
- Start required services
- Create databases as needed
- Install Python dependencies
- Execute the corresponding demo logic

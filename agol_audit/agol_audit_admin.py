"""
===============================================================================
Script Name:       AGOL Organization Audit (Admin)
Author:            Andrew Sheppard
Role:              GIS Developer | Solutions Engineer
Email:             andrewsheppard8@gmail.com
Date Created:      2026-04-09

Description:
-------------
This script performs an organization-wide audit of ArcGIS Online content.

    - Authenticates as an AGOL administrator
    - Iterates through all users in the organization
    - Retrieves each user's content
    - Calculates:
        * Item count per user
        * Storage usage per user (approximate)
    - Identifies:
        * Largest users by storage
        * Users with no content
    - Outputs results to console and exports a timestamped .txt report

Requirements:
--------------
- Must be run with an account that has administrative privileges

Future Improvements:
--------------------
- Export to CSV/Excel for reporting
- Include group and sharing analysis
- Add credit usage reporting (admin-only)
- Parallelize user processing for large orgs

===============================================================================
"""

from arcgis.gis import GIS
import os
from datetime import datetime

# ==============================
# CONFIG (ADMIN REQUIRED)
# ==============================
PORTAL_URL = "portal_url" #UPDATE
USERNAME = "username" #UPDATE
PASSWORD = "password" #UPDATE

# ==============================
# LOG BUFFER
# ==============================
log_output = []

def log(msg=""):
    print(msg)
    log_output.append(str(msg))

# ==============================
# EXPORT
# ==============================
def export_report():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    report_path = os.path.join(
        os.path.abspath(os.path.dirname(__file__)),
        f"agol_org_audit_{timestamp}.txt"
    )

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(log_output))

    print(f"\nReport saved to:\n{report_path}")

# ==============================
# MAIN
# ==============================
def audit_org():
    log("Connecting to AGOL as admin...")
    gis = GIS(PORTAL_URL, USERNAME, PASSWORD)

    org_users = gis.users.search(max_users=10000)
    total_users = len(org_users)

    log(f"Total users found: {total_users}\n")

    user_summaries = []

    for i, user in enumerate(org_users, start=1):
        log(f"[{i}/{total_users}] Processing: {user.username}")

        try:
            items = gis.content.search(
                query=f"owner:{user.username}",
                max_items=1000
            )

            item_count = len(items)
            total_size = sum(item.size or 0 for item in items)
            total_size_mb = total_size / (1024 ** 2)

            user_summaries.append({
                "username": user.username,
                "items": item_count,
                "size_mb": total_size_mb
            })

        except Exception as e:
            log(f"Error processing {user.username}: {e}")

    # ==============================
    # SUMMARY STATS
    # ==============================
    log("\n===== ORG SUMMARY =====")

    total_items = sum(u["items"] for u in user_summaries)
    total_storage = sum(u["size_mb"] for u in user_summaries)

    log(f"Total Users: {total_users}")
    log(f"Total Items: {total_items}")
    log(f"Total Storage (approx): {total_storage:.2f} MB")

    # ==============================
    # TOP USERS BY STORAGE
    # ==============================
    log("\n--- Top 10 Users by Storage ---")

    top_users = sorted(
        user_summaries,
        key=lambda x: x["size_mb"],
        reverse=True
    )[:10]

    for i, u in enumerate(top_users, start=1):
        log(f"{i}. {u['username']} - {u['size_mb']:.2f} MB ({u['items']} items)")

    # ==============================
    # USERS WITH NO CONTENT
    # ==============================
    log("\n--- Users With No Content ---")

    empty_users = [u for u in user_summaries if u["items"] == 0]

    log(f"{len(empty_users)} users have no content")

    # ==============================
    # EXPORT
    # ==============================
    export_report()


# ==============================
# RUN
# ==============================
if __name__ == "__main__":
    audit_org()

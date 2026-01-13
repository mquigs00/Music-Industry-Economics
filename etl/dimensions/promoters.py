import slugify
from config.paths import DIM_PROMOTERS_PATH
import csv

def update_dim_promoters(promoter_names, dim_promoters):
    existing_promoters = dim_promoters["by_slug"]
    max_promoter_id = dim_promoters["max_id"]

    for name in promoter_names:
        slug = slugify.slugify(name)
        if slug not in existing_promoters:
            max_promoter_id += 1
            append_dim_promoters(name, slug, max_promoter_id)
            dim_promoters[slug] = {
                "id": max_promoter_id,
                "name": name,
                "slug": slug
            }
            dim_promoters["max_id"] = max_promoter_id

def append_dim_promoters(name, slug, next_id):
    with open(DIM_PROMOTERS_PATH, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([next_id, name, slug, None])

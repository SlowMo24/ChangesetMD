docker compose run --rm changesetmd \
-H db \
-P 5432 \
-u changesets \
-p changesets \
-d changesets \
-s public \
-f ./data/discussions-250210.osm.bz2 \
-g \
-c
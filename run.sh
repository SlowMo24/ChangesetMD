docker compose pull
docker compose build
docker compose up -d db
docker compose run -v $PWD/postgres/is_ready.sh:/is_ready.sh --rm db sh -c "/is_ready.sh"
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
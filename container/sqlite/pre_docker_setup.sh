# Set up paths

mkdir -p /tmp/store/libstore/staging
mkdir -p /tmp/store/libstore/store
mkdir -p /tmp/store/libclone/staging
mkdir -p /tmp/store/libclone/store

# We need to copy in the alembic to here, so we can run the
# database migration.

cp -r ../../alembic .
cp ../../alembic.ini .

echo "You need to change the value of YOUR_HOSTNAME to: $(hostname) in server_config.json"

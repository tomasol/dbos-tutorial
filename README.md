# Serial and parallel workflow demo

Based on [Learn DBOS Java Tutorial](https://docs.dbos.dev/java/programming-guide).

## Set up
If using nix flakes and direnv:
```
cp .envrc-example .envrc
$EDITOR .envrc
direnv allow
```
Otherwise install dependencies as described in [dev-deps.txt](dev-deps.txt)

Run Postgres:
```sh
docker run -it --rm \
  --name dbos-postgres \
  -e POSTGRES_PASSWORD=dbos \
  -p 5432:5432 \
  postgres:17
```

Run app
```sh
gradle run
```

.PHONY: fmt test lint docs-check build release docker

fmt:
	cargo fmt --all -- --check

test:
	cargo test --workspace

lint:
	cargo clippy --workspace --all-targets -- -D warnings

docs-check:
	@test -f README.md
	@test -f docs/DESIGN.md
	@test -f docs/USAGE.md
	@test -f docs/LIMITATIONS.md
	@echo "documentation files present"

build:
	cargo build --locked

release:
	cargo build --release --locked

docker:
	docker build -t tsdb2parqtel:local .

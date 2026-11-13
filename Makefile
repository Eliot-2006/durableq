DATABASE_URL ?= postgres://durableq:durableq@localhost:5432/durableq?sslmode=disable
export DATABASE_URL

.PHONY: up down migrate test vet chaos demo

up:
	docker compose up -d --wait postgres

down:
	docker compose down

migrate:
	go run ./cmd/durableq migrate

vet:
	go vet ./...

test:
	go test -race ./...

chaos:
	go test -tags chaos -timeout 10m ./chaos/...

demo:
	docker compose run --rm demo

KUBECONFORM := github.com/yannh/kubeconform/cmd/kubeconform@v0.6.7
CHART := deploy/helm/durableq

.PHONY: docker-build helm-lint kind-up kind-deploy kind-down k8s-smoke

docker-build:
	docker build -t durableq:dev .

helm-lint:
	@for f in values-kind.yaml values-ci.yaml; do \
	  helm lint $(CHART) -f $(CHART)/$$f || exit 1; \
	  helm template durableq $(CHART) -f $(CHART)/$$f | go run $(KUBECONFORM) -strict -ignore-missing-schemas -summary || exit 1; \
	done
	helm lint $(CHART)
	helm template durableq $(CHART) | go run $(KUBECONFORM) -strict -ignore-missing-schemas -summary

kind-up:
	scripts/kind-up.sh

# Rebuild the image and roll the chart on an existing cluster.
kind-deploy:
	DEPLOY_ONLY=1 scripts/kind-up.sh

kind-down:
	scripts/kind-down.sh

k8s-smoke:
	scripts/k8s-smoke.sh

.PHONY: k8s-chaos k8s-bench

k8s-chaos:
	scripts/k8s-chaos.sh

k8s-bench:
	scripts/k8s-bench.sh

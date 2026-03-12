# 🛍️ Kafka E-commerce Microservices Platform

[![CI](https://github.com/YOUR_USERNAME/kafka-ecommerce/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/kafka-ecommerce/actions/workflows/ci.yml)
[![Deploy Dev](https://github.com/YOUR_USERNAME/kafka-ecommerce/actions/workflows/deploy-dev.yml/badge.svg)](https://github.com/YOUR_USERNAME/kafka-ecommerce/actions/workflows/deploy-dev.yml)
[![codecov](https://codecov.io/gh/YOUR_USERNAME/kafka-ecommerce/branch/main/graph/badge.svg)](https://codecov.io/gh/YOUR_USERNAME/kafka-ecommerce)

A production-ready event-driven e-commerce platform built with Golang, Kafka, and Docker.

## 🏗️ Architecture
```
Order → Inventory → Payment → Shipping
  ↓         ↓          ↓          ↓
        Kafka Event Stream
  ↓         ↓          ↓          ↓
Notifications    Analytics Service
```

## 🚀 Quick Start
```bash
# Start all services
docker-compose up -d

# Create an order
curl -X POST http://localhost:8082/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"cust-123","items":[{"product_id":"prod-001","quantity":1,"price":99.99}],"currency":"USD"}'

# View analytics
curl http://localhost:8091/api/metrics
```

## 📊 Services

| Service | Port | Description |
|---------|------|-------------|
| Order Service | 8082 | Order management |
| Inventory Service | -    | Stock reservation |
| Payment Service | -    | Payment processing |
| Shipping Service | -    | Order fulfillment |
| Notification Service | -    | Customer notifications |
| Analytics Service | 8091 | Real-time metrics |

## 🔄 CI/CD Pipeline

### Continuous Integration
- ✅ Automated testing on every PR
- ✅ Code coverage tracking
- ✅ Security scanning
- ✅ Docker image builds

### Continuous Deployment
- 🔵 Auto-deploy to Dev on merge to `main`
- 🟢 Manual approval for Production
- 🔄 Automatic rollback on failure

## 📈 Monitoring

- Real-time dashboard: http://localhost:8090
- Kafka UI: http://localhost:8090

## 🧪 Testing
```bash
# Run all tests
make test

# Run specific service tests
cd services/order-service
go test -v ./...
```

## 🚢 Deployment

### Development
```bash
git push origin main
# Automatically deploys to dev
```

### Production
```bash
# Create release
gh workflow run release.yml -f version=v1.0.0

# This triggers production deployment with approval
```

## 📦 Docker Images

All services available at:
```
docker.io/YOUR_USERNAME/order-service:latest
docker.io/YOUR_USERNAME/inventory-service:latest
...
```

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

## 📝 License

MIT License - see LICENSE file for details
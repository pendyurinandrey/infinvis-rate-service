storage "postgresql" {
  connection_url = "postgres://postgres:password@postgres:5432/vault_db"
}
listener "tcp" {
  address     = "0.0.0.0:8200"
  tls_disable = 1
}
ui = true
api_addr = "https://0.0.0.0:8200"

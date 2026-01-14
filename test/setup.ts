import { ClickHouseContainer, type StartedClickHouseContainer } from "@testcontainers/clickhouse";

let container: StartedClickHouseContainer | undefined;

export async function startClickHouse(version = "25.8") {
  console.log("Starting ClickHouse container...");

  container = await new ClickHouseContainer(`clickhouse/clickhouse-server:${version}`)
    .withDatabase("default")
    .withUsername("default")
    .withPassword("password")
    .start();

  const host = container.getHost();
  const port = container.getMappedPort(8123);
  const tcpPort = container.getMappedPort(9000);
  const url = `http://${host}:${port}`;

  console.log(`ClickHouse started at ${url} (TCP: ${tcpPort})`);
  return {
    container,
    url,
    host,
    port,
    tcpPort,
    username: "default",
    password: "password",
  };
}

export async function stopClickHouse() {
  if (container) {
    console.log("Stopping ClickHouse container...");
    await container.stop();
  }
}

import { ClickHouseContainer, StartedClickHouseContainer } from "@testcontainers/clickhouse";

let container: StartedClickHouseContainer | undefined;

export async function startClickHouse() {
  console.log("Starting ClickHouse container...");

  container = await new ClickHouseContainer(
    "clickhouse/clickhouse-server:latest",
  )
    .withDatabase("default")
    .withUsername("default")
    .withPassword("password")
    .start();

  const host = container.getHost();
  const port = container.getMappedPort(8123);
  const url = `http://${host}:${port}`;

  console.log(`ClickHouse started at ${url}`);
  return {
    container,
    url,
    host,
    port,
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

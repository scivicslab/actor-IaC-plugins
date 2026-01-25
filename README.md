# actor-IaC-plugins

Standard plugins for actor-IaC, providing advanced log aggregation and system information analysis.

## Installation

```bash
git clone https://github.com/scivicslab/actor-IaC-plugins
cd actor-IaC-plugins
mvn package
```

Copy the built JAR to your actor-IaC project:

```bash
cp target/actor-IaC-plugins-1.0.0.jar ~/your-project/plugins/
```

## SystemInfoAggregator

A plugin for aggregating system information from multiple nodes into formatted tables.

### Actions

| Action | Description |
|--------|-------------|
| `connect` | Connect to the H2 log database |
| `disconnect` | Disconnect from the database |
| `summarize-gpus` | Aggregate GPU information into a formatted table |
| `summarize-disks` | Aggregate disk information into a formatted table |
| `node-status` | Display node status |
| `list-sessions` | List available sessions |

### Usage in Workflow

```yaml
# Load the plugin JAR
- actor: loader
  method: loadJar
  arguments: ["plugins/actor-IaC-plugins-1.0.0.jar"]

# Create the aggregator actor
- actor: loader
  method: createChild
  arguments: ["loader", "aggregator", "com.scivicslab.actoriac.plugins.h2analyzer.SystemInfoAggregator"]

# Connect to database
- actor: aggregator
  method: connect
  arguments: ["./actor-iac-logs"]

# Summarize GPU information
- actor: aggregator
  method: summarize-gpus
  arguments: []
```

### Example Output

```
## GPU Summary
| node | gpu | vram | driver | cuda | compute_cap |
|------|-----|------|--------|------|-------------|
| 192.168.5.13 | NVIDIA GeForce RTX 4080 | 16GB | 550.54.14 | 12.4 | 8.9 |
| 192.168.5.14 | NVIDIA GeForce RTX 4080 | 16GB | 550.54.14 | 12.4 | 8.9 |
| 192.168.5.15 | NVIDIA GB10 | 128GB | 550.54.14 | 12.4 | 10.0 |
```

## License

Apache License 2.0

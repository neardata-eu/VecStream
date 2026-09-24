# Benchmark Clients

One parameterized Terraform + Ansible stack that provisions the EC2 benchmark client used by the ingestion, static queries and streaming queries suites. The suite is selected by the active terraform workspace, so a single terraform root serves all three suites with separate state, AWS tags and SSH keys.

## What It Provisions

| Piece | Controlled by | Details |
|-------|---------------|---------|
| EC2 client instance | `enable_client` (default `true`) | `m6i.large` in `us-east-1a` by default; name and tags come from the workspace |
| SSH key pair | `enable_client` | TLS key registered as an `aws_key_pair` named per suite; private key written to `bench-key-<workspace>.pem` (mode 0600) next to `main.tf` |
| S3 + S3 Express buckets | `enable_s3` (default `false`) | Standard bucket (`s3_bucket_name`) and S3 Express One Zone directory bucket (`express_bucket_name`) |
| IAM policies | automatic | Dataset bucket access with the client; S3 and Express access when both client and buckets are enabled |
| Ansible setup | n/a | `ansible/` holds one playbook pair and three dynamic inventories that configure the instance from the repo |

`terraform/session-manager-plugin.deb` is bundled in the stack directory (carried over from the old ingestion deployment). No terraform resource installs it; copy it to the client yourself if you want AWS Session Manager access.

## Prerequisites

- Terraform >= 1.5
- AWS CLI v2, authenticated against the target account
- Ansible 2.14+ with the `amazon.aws` collection (the inventories use its `aws_ec2` dynamic inventory plugin)
- Python and ssh on the control machine

The old per-suite stacks shipped a `check_prereqs.sh` script; it was removed in the merge because it also checked Kafka-stack tools (kubectl, helm, eksctl) that this stack does not need. The relevant subset is listed above.

## Suite Selection via Terraform Workspace

The suite comes from `terraform.workspace` alone. There is no separate `suite` variable. The three valid workspaces are `ingestion`, `static-queries` and `streaming-queries`. Applying in the `default` workspace, or any name that is not one of the three, fails loudly: the locals match the workspace against `^(ingestion|static-queries|streaming-queries)$` with a `regex()` guard, and terraform errors when the pattern does not match. (This replaces the planned `error()` call, which is not available in the terraform version used to build the stack.)

Each workspace maps to one entry in the `suite_config` locals:

| Workspace | `Project` tag | Instance name | Key pair name |
|-----------|---------------|---------------|---------------|
| `ingestion` | `ingestion-bench` | `bench-client-ingestion` | `bench-key-ingestion` |
| `static-queries` | `static-queries-bench` | `bench-client` | `bench-key` |
| `streaming-queries` | `streaming-queries-bench` | `streaming-bench-client` | `streaming-bench-key` |

The `Project` tag is what the ansible inventories filter on, and the instance name becomes the `Role` tag that the playbooks rely on for group targeting.

## Deploy

```bash
cd deployment/benchmark_clients/terraform

terraform init    # first init re-resolves providers, see "Provider Lock" below

terraform workspace new ingestion     # or: terraform workspace select ingestion
terraform apply
```

Each workspace keeps its own state, so the suites never collide. The private key lands in `terraform/bench-key-<workspace>.pem`.

Variables worth knowing (full list in `terraform/variables.tf`):

- `region`, `instance_type`, `availability_zone`: placement defaults, `us-east-1` / `m6i.large` / `us-east-1a`.
- `enable_client` (bool): set `false` to manage only the buckets, without an EC2 client.
- `enable_s3` (bool): set `true` for the plain-S3 benchmark buckets.
- `vpc_id`, `subnet_id`: leave empty (the default) to use the account's default VPC and the subnet matching `availability_zone`. The merged stack standardizes on the `""` default style from the old streaming stack; the ingestion and static-queries stacks had `null` here, which terraform treats the same way for these string variables.
- `dataset_s3_bucket_name`: bucket the client's IAM role may read (default `vecstream-benchmarks-data`).

## Provision the Client with Ansible

Run ansible from the `ansible/` directory so the relative key paths in the inventories resolve:

```bash
cd deployment/benchmark_clients/ansible

# Full setup: apt packages, uv, Python 3.13, repo synced to /opt/vecstream
ansible-playbook -i inventory/aws_ec2_ingestion.yml playbooks/client.yml

# After code changes: rsync benchmarks/, vecstream/ and pyproject.toml, then uv sync
ansible-playbook -i inventory/aws_ec2_ingestion.yml playbooks/resync-code.yml
```

The suite is selected by the inventory file: `aws_ec2_ingestion.yml`, `aws_ec2_static_queries.yml` or `aws_ec2_streaming_queries.yml`. Each filters EC2 on its suite's `Project` tag and sets `ansible_private_key_file` (via the plugin's `compose:`) to the matching `bench-key-<suite>.pem` under `../terraform/`. `ansible.cfg` deliberately hardcodes neither an inventory nor a key file; the old shared `bench-key.pem` collision is gone because terraform writes one key file per workspace.

`client.yml` syncs the repo to `/opt/vecstream` on the client, then runs suite-gated steps based on the `tag_Project_*` groups the inventory plugin builds:

- ingestion: copies local dataset samples from `datasets/article_validation/samples/`
- static-queries: downloads the datasets with `aws s3 sync` from `s3://vecstream-benchmarks-data/`
- streaming-queries: syncs local query and sample files from `datasets/article_validation/`

The `datasets/` paths are user-provided content (gitignored), created by the dataset prep of each suite.

## SSH to the Client

```bash
cd deployment/benchmark_clients/terraform
terraform output ssh_command
```

The output is a ready `ssh -i bench-key-<workspace>.pem ubuntu@<ip>` command for the current workspace. `client_public_ip`, `client_instance_id` and `client_security_group_id` are also exported.

## Dropped in the Merge

- `check_prereqs.sh`: removed entirely; see Prerequisites above.
- Streaming's `s3://YOUR_BUCKET/kafka/` dataset sync: dropped because the placeholder fails if executed. If you need it, add a task to `client.yml` gated on the streaming Project group and point it at a real bucket.
- Commented-out dead blocks from the old per-suite playbooks: not carried over.

## Provider Lock

`.terraform.lock.hcl` was deleted with the old per-suite stacks, so the first `terraform init` in `terraform/` re-resolves provider versions against the `>=` constraints in `main.tf`. The resolved versions may differ from the pins the authors tested with.

## Teardown

Destroy each suite you applied, from the terraform directory:

```bash
cd deployment/benchmark_clients/terraform

terraform workspace select ingestion
terraform destroy
```

Repeat with `terraform workspace select static-queries` and `terraform workspace select streaming-queries` as needed; each workspace destroys only its own resources. The generated `bench-key-<workspace>.pem` files stay on disk and can be deleted manually.

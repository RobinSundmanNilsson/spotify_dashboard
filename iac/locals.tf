locals {
  # Parse the subscription id from env_variable.sh so runs don't require sourcing the file.
  env_file_content          = try(file("${path.module}/env_variable.sh"), "")
  subscription_line         = try(regex("ARM_SUBSCRIPTION_ID\\s*=\\s*\"[^\"]+\"", local.env_file_content), "")
  subscription_id_from_file = length(trimspace(local.subscription_line)) > 0 ? trimsuffix(trimprefix(trimspace(local.subscription_line), "ARM_SUBSCRIPTION_ID=\""), "\"") : null

  pipeline_sources_hash = sha1(
    join(
      "",
      concat(
        [
          filesha1("${path.module}/../dockerfile.dwh"),
          filesha1("${path.module}/../requirements_mac.txt"),
        ],
        [
          for f in sort(fileset("${path.module}/../data_extract_load", "**/*.py")) :
          filesha1("${path.module}/../data_extract_load/${f}")
        ],
        [
          for f in sort(fileset("${path.module}/../orchestration", "**/*.py")) :
          filesha1("${path.module}/../orchestration/${f}")
        ],
        [
          for f in sort(fileset("${path.module}/../dbt_spotify_duckdb", "**")) :
          filesha1("${path.module}/../dbt_spotify_duckdb/${f}")
          if !startswith(f, "target/") && !startswith(f, "logs/") && !startswith(f, "dbt_packages/")
        ],
      )
    )
  )

  dashboard_sources_hash = sha1(
    join(
      "",
      concat(
        [
          filesha1("${path.module}/../dockerfile.dashboard"),
        ],
        [
          for f in sort(fileset("${path.module}/../dashboard", "**/*.py")) :
          filesha1("${path.module}/../dashboard/${f}")
        ],
      )
    )
  )

  pipeline_image_tag  = substr(local.pipeline_sources_hash, 0, 12)
  dashboard_image_tag = substr(local.dashboard_sources_hash, 0, 12)
}

library(tidyverse)

errors <- read_csv('release4/error_list.csv') |>
    janitor::clean_names() |>
    mutate(
        error = str_split(errors, "(?<!'),"),
        htan_center = str_remove(htan_center, 'HTAN ')
    ) |>
    select(-errors) |>
    unnest(error) |>
    mutate(
        generic_error = str_replace_all(
            error,
            c("HTA\\d+\\_\\d+_\\d+" = "HTAN ID", 
              "\\['syn(.*?)'\\]" = ""
            )
        ) |> str_trim(),
        error = str_trim(error) 
    ) |>
    mutate(
      filename = NULL,
      manifest_version = NULL,
      uuid = NULL
    )
  

rate <- rate_delay(5)


errors |> group_nest(htan_center, manifest_id, component, generic_error, sort = TRUE) |> 
    rename(error_table = data) |>
    mutate(
        count = map_int(error_table, nrow),
        title = str_glue('{htan_center} | {component} | {count} errors: {generic_error}'),
        error_table = map(error_table, ~knitr::kable(.x) %>% 
            paste(collapse = "\n")),
        body = str_glue('HTAN Center: {htan_center}
        Component: {component}
        Manifest ID: [{manifest_id}](https://www.synapse.org/#!Synapse:{manifest_id})
        {error_table}'),
        issue = map2(
            title,body,
            slowly(
            ~gh::gh(
                title = .x, 
                body = .y, 
                endpoint = "POST /repos/ncihtan/data-release-tracker/issues"
            ),
            rate = rate, quiet = FALSE)
        )
    )

# https://hudi.apache.org/docs/configurations.html
def get_operation_options(overrides: dict) -> dict:
    options = {
        'hoodie.datasource.write.partitionpath.field': 'partitionpath',
        'hoodie.upsert.shuffle.parallelism': 2,
        'hoodie.insert.shuffle.parallelism': 2
    }

    options.update(overrides)
    if 'hoodie.datasource.write.table.name' not in options:
        options['hoodie.datasource.write.table.name'] = options['hoodie.table.name']

    return options

def cancel_clickhouse_query(client, query_id):
    """Cancel matching active work; this is not a fence against later requests."""
    return client.command(
        "KILL QUERY WHERE query_id = {query_id:String} SYNC",
        parameters={"query_id": query_id},
    )

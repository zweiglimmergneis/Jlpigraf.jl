#
# Functions for fetching data from Epigraf
#

"""
Fetch entity data such as articles, projects or properties from the API

Returns all data belonging to all entities matched by the params.
The procedure corresponds to calling the index action
with the columns parameter set to 0 in the Epigraf interface.

Arguments:
- table: The table name (e.g. "articles")
- params: A named list of query params
- db: The database name
- maxpages: Maximum number of pages to request. Set to 1 for non-paginated tables.
"""
function api_fetch(table, params = Dict(), db = nothing, maxpages = 1)
    params["columns"] = "0"
    params["idents"] = "id"
    df = api_table(table, params, db, maxpages)
    df.table = [match(r"^[a-z]+", id).match for id in df.id]
    df.database = db
    df = unique(df)
    df = move_cols_to_front(df, ["database", "table", "type", "id"])
    return df
end

"""
Fetch entity data such as articles, projects or properties using direct database access

Returns all data belonging to all entities matched by the params.

Arguments:
- table: The table name (e.g. "articles")
- params: A named list of query conditions
- db: The database name
"""
function db_fetch(table, params = Dict(), db = nothing)
    df_root = db_table(table, params, db = db, compact = true)
    df = df_root

    if (table == "articles") && (nrow(df_root) > 0)
        df_root.project = df_root.projects_id
        df_root.projects_id = nothing

        df.project = df.projects_id
        df.projects_id = nothing

        df_sections = db_table("sections", Dict("articles_id" => df_root.id), db = db, compact = true)
        df = vcat(df, df_sections)

        df_items = db_table("items", Dict("articles_id" => df_root.id), db = db, compact = true)
        df_items.property = df_items.properties_id
        df_items.properties_id = nothing
        df = vcat(df, df_items)

        items_props = df_items[.!ismissing.(df_items.property), :property]
        if length(items_props) > 0
            df_props = db_table("properties", Dict("id" => items_props), db = db, compact = true)
            df = vcat(df, df_props)
        end

        df_footnotes = db_table("footnotes", Dict("root_tab" => "articles", "root_id" => df_root.id), db = db, compact = true)
        df = vcat(df, df_footnotes)

        df_links = db_table("links", Dict("root_tab" => "articles", "root_id" => df_root.id), db = db, compact = true)
        df = vcat(df, df_links)

        links_props = filter(row -> row.to_tab == "properties" && !ismissing(row.to_id), df_links)
        if nrow(links_props) > 0
            df_props = db_table("properties", Dict("id" => links_props.to_id), db = db, compact = true)
            df = vcat(df, df_props)
        end

        df_projects = db_table("projects", Dict("id" => df_root.project), db = db, compact = true)
        df = vcat(df, df_projects)
    end

    df = drop_empty_columns(df)
    df = move_cols_to_front(df, ["database", "table", "type", "id"])
    return df
end

"""
Fetch tables such as articles, projects or properties

Returns a row with defined columns for each record matched by the params.
The procedure corresponds to calling the index action in the Epigraf interface.

Arguments:
- table: The table name (e.g. "articles")
- columns: A vector of column names.
- params: A named list of query params
- db: The database name
- maxpages: Maximum number of pages to request. Set to 1 for non-paginated tables.
"""
function fetch_table(table, columns = [], params = Dict(), db = nothing, maxpages = 1)
    columns = unique(vcat(["id"], columns))
    columns_str = join(columns, ",")
    params["columns"] = columns_str
    params["idents"] = "id"
    return api_table(table, params, db, maxpages)
end

"""
Fetch entities such as single articles, projects or properties

Returns all data belonging to the entity identified by ID.
The procedure corresponds to calling the view action in the Epigraf interface.

Arguments:
- ids: A character vector with IDs as returned by fetch_table, e.g. articles-1.
         Alternatively, provide a dataframe containing the IDs in the id-column.
         So you can chain fetch_articles() and fetch_entity()
- params: A named list of query params
- db: The database name. Leave empty when providing a dataframe produced by fetch_table().
        In this case, the database name will be extracted from the dataframe.
- silent: Whether to output a progress bar
"""
function fetch_entity(ids, params = Dict(), db = nothing, silent = false)
    if isnothing(db) && hasproperty(ids, :source)
        db = ids.source["db"]
    end

    if !isnothing(db)
        check_is_db(db)
    end

    if isa(ids, DataFrame)
        ids = ids.id
    end

    if length(ids) > 1
        if !silent
            println("Fetching data...")
        end
        data = DataFrame()

        for id in ids
            data = vcat(data, fetch_entity(id, params, db, silent = true))
        end

        return data
    end

    if length(ids) == 0
        data = to_epitable(DataFrame(), Dict("params" => params, "db" => db))
        return data
    end

    check_is_id(ids)
    id_parts = split(ids, "-")
    table = id_parts[1]
    id = id_parts[2]

    data = api_table(string(table, "/view/", id), params, db, 1, silent = silent)
    data = separate_wider_delim(data, :id, "-", ["table", "row"])
    return to_epitable(data)
end

function move_cols_to_front(df, cols)
    remaining_cols = setdiff(names(df), cols)
    return select(df, vcat(cols, remaining_cols))
end

function drop_empty_columns(df)
    return select(df, [col for col in names(df) if any(!ismissing, df[!, col])])
end

function check_is_db(db)
    return true
end

function check_is_id(id)
    return true
end

function separate_wider_delim(df, col, delim, names)
    return df
end
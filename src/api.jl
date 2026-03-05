#
# Functions for low level API access to Epigraf
#

"""
    api_setup(apiserver, apitoken = nothing, verbose = false)

Save API connection settings to environment variables.

Arguments:
- apiserver: URL of the Epigraf server (including https-protocol)
- apitoken: Access token. If NULL, you will be asked to enter the token.
- verbose: Show debug messages and the built URLs
"""
function api_setup(apiserver, apitoken = nothing, verbose = false)
    if isnothing(apitoken)
        print("Please, enter your access token: ")
        apitoken = readline()
    end
    settings = Dict("apiserver" => apiserver, "apitoken" => apitoken, "verbose" => verbose)
    for (key, value) in settings
        ENV["epi_" * string(key)] = string(value)
    end
end

"""
Set silent mode

In silent mode, all user prompts are automatically confirmed.
Be careful, this will skip the prompt to confirm operations
on the live server.

Arguments:
- silent: Boolean
"""
function api_silent(silent = false)
    ENV["epi_silent"] = string(silent)
end

"""
Build base URL

Arguments:
- endpoint: The endpoint, e.g. articles/import
- query: Query parameters for the endpoint
- database: The database name
- extension: Extension added to the URL path, defaults to json.
"""
function api_buildurl(endpoint, query = nothing, database = nothing, extension = "json")
    server = get(ENV, "epi_apiserver", "")
    token = get(ENV, "epi_apitoken", "")
    verbose = get(ENV, "epi_verbose", "false") == "true"
    silent = get(ENV, "epi_silent", "false") == "true"

    url = URI(server)
    url.query = Dict("token" => token)

    if !isnothing(query)
        for (k, v) in query
            url.query[k] = v
        end
    end

    if !startswith(endpoint, "/")
        endpoint = "/" * endpoint
    end

    parsed_endpoint = URI(endpoint)
    for (k, v) in parsed_endpoint.query
        url.query[k] = v
    end
    endpoint = parsed_endpoint.path

    endpoint_extension = splitext(endpoint)[2]
    if endpoint_extension != ""
        extension = ""
    elseif isnothing(extension)
        extension = ""
    elseif !isnothing(extension) && !startswith(extension, ".")
        extension = "." * extension
    end

    if !isnothing(database)
        url.path = "epi/" * database * endpoint * extension
    else
        url.path = endpoint * extension
    end

    built_url = string(url)
    if verbose && !silent
        println(built_url)
    end

    return built_url
end

"""
Create and execute a job

Arguments:
- endpoint: The endpoint supporting job creation
- params: Query parameters
- database: The selected database
- payload: The data posted to the job endpoint
"""
function api_job_create(endpoint, params, database, payload = nothing)
    verbose = get(ENV, "epi_verbose", "false") == "true"
    server = get(ENV, "epi_apiserver", "")

    silent = get(ENV, "epi_silent", "false")
    if silent != "true"
        println("Creating job on server " * server)
    end

    if !isLocalServer(server)
        confirmAction()
    end

    url = api_buildurl(endpoint, params, database)

    headers = Dict("Content-Type" => "application/json")
    if verbose
        resp = HTTP.post(url, headers, JSON.json(payload))
    else
        resp = HTTP.post(url, headers, JSON.json(payload))
    end

    body = JSON.parse(String(resp.body))
    job_id = get(body, "job_id", nothing)

    error = false
    message = nothing

    if resp.status != 200
        error = true
        message = get(body, "error", Dict()).get("message", nothing)
    elseif get(body, "success", true) != true
        error = true
        message = get(body, "message", nothing)
    elseif isnothing(job_id)
        error = true
        message = "No job ID found."
    end

    if error
        throw(ErrorException("Could not create job: " * string(message)))
    end

    if !isnothing(message)
        println(message)
    end

    api_job_execute(job_id)
end

"""
Execute a job

Arguments:
- job_id: The job ID
"""
function api_job_execute(job_id)
    verbose = get(ENV, "epi_verbose", "false") == "true"
    println("Starting job " * string(job_id) * ".")

    url = api_buildurl("jobs/execute/" * string(job_id))

    result = []
    polling = true
    while polling
        if verbose
            resp = HTTP.post(url)
        else
            resp = HTTP.post(url)
        end

        body = JSON.parse(String(resp.body))
        newresult = nothing

        if resp.status != 200
            polling = false
            error = true
            message = get(body, "error", Dict()).get("message", nothing)
        elseif get(body, "job", Dict()).get("error", false) != false
            polling = false
            error = true
            message = get(body, "job", Dict()).get("error", nothing)
        elseif !isnothing(get(body, "job", Dict()).get("nextUrl", nothing))
            polling = true
            error = false
            message = get(body, "job", Dict()).get("message", nothing)
            newresult = get(body, "job", Dict()).get("result", nothing)

            delay = get(body, "job", Dict()).get("delay", 0)
            if delay > 0
                sleep(1)
            end

            progressCurrent = get(body, "job", Dict()).get("progress", nothing)
            progressMax = get(body, "job", Dict()).get("progressmax", -1)
            if progressMax == -1
                println("Progress " * string(progressCurrent))
            else
                println("Progress " * string(progressCurrent) * " / " * string(progressMax))
            end
        else
            polling = false
            error = false
            message = get(body, "message", nothing)
            newresult = get(body, "job", Dict()).get("result", nothing)
        end

        if !isnothing(newresult)
            push!(result, newresult)
        end

        if !isnothing(message)
            println(message)
        end
    end

    solved = []
    if length(result) > 0
        solved = vcat([DataFrame(x["solved"]) for x in result]...)
        result = [delete!(x, "solved") for x in result]
    end

    downloads = []
    if length(result) > 0
        downloads = vcat([DataFrame(x["downloads"]) for x in result]...)
        result = [delete!(x, "downloads") for x in result]
    end

    return (polling = polling, error = error, message = message, data = result, solved = solved, downloads = downloads)
end

"""
Download tables

Fetches tables such as articles, projects or properties

Arguments:
- endpoint: The endpoint path (e.g. "articles/index" or "articles/view/1")
- params: A named list of query params
- db: The database name
- maxpages: Maximum number of pages to request. Set to 1 for non-paginated tables.
- silent: Whether to output status messages
"""
function api_table(endpoint, params = Dict(), db = nothing, maxpages = 1, silent = false)
    verbose = get(ENV, "epi_verbose", "false") == "true"

    data = DataFrame()
    page = 1

    fetchmore = true
    while fetchmore
        params["page"] = page
        url = api_buildurl(endpoint, params, db, "csv")

        if !silent
            if maxpages == 1
                println("Fetching data from " * endpoint * ".")
            else
                println("Fetching page " * string(page) * " from " * endpoint * ".")
            end
        end
        message = nothing

        try
            if verbose
                resp = HTTP.get(url)
            else
                resp = HTTP.get(url)
            end

            if resp.status == 200
                body = String(resp.body)
                rows = CSV.read(IOBuffer(body), delim = ';')
            elseif resp.status == 404
                message = "No more data found."
                rows = DataFrame()
            else
                rows = DataFrame()
                message = "Error " * string(resp.status) * ": " * String(resp.body)
            end
        catch e
            message = string(e)
            rows = DataFrame()
        end

        if !isnothing(message)
            println(message)
        end

        if nrow(rows) > 0
            data = vcat(data, rows)
            fetchmore = (page < maxpages)
            page += 1
        else
            fetchmore = false
        end
    end

    if !silent
        println("Fetched " * string(nrow(data)) * " records from " * endpoint * ".")
    end

    if nrow(data) > 0
        for col in names(data)
            if eltype(data[!, col]) == String
                try
                    data[!, col] = parse.(eltype(data[!, col]), data[!, col])
                catch
                end
            end
        end
    end

    return to_epitable(data, Dict("endpoint" => endpoint, "params" => params, "db" => db))
end

"""
Post data to epigraf

Arguments:
- endpoint: The endpoint path
- params: Query parameters
- payload: The data posted to the endpoint
- database: The selected database
"""
function api_post(endpoint, params = Dict(), payload = nothing, database = nothing)
    result = api_request(endpoint, params, payload, database, HTTP.post)
    return result
end

"""
Upload file to epigraf

Arguments:
- endpoint: The endpoint path
- params: Query parameters
- filepath: A full path to the local file
- mimetype: The mime type of the file. Will be guessed if empty.
- overwrite: Whether to overwrite existing files.
- database: The selected database
"""
function api_upload(endpoint, params = Dict(), filepath = nothing, mimetype = nothing, overwrite = false, database = nothing)
    payload = Dict("FileData[0]" => filepath, "FileOverwrite" => overwrite ? "1" : "0")
    result = api_request(endpoint, params, payload, database, HTTP.post, "multipart")
    return result
end

"""
Download a file from Epigraf

Arguments:
- endpoint: The endpoint path.
- params: Query parameters.
- filename: A file name or a full path to the local file.
- filepath: A target folder path
- overwrite: Whether to overwrite existing files.
- database: The selected database.
"""
function api_download(endpoint, params = Dict(), filename = nothing, filepath = nothing, overwrite = false, database = nothing)
    verbose = get(ENV, "epi_verbose", "false") == "true"
    server = get(ENV, "epi_apiserver", "")

    silent = get(ENV, "epi_silent", "false")
    if silent != "true"
        println("Downloading file from " * server)
    end

    destfile = isnothing(filepath) ? filename : joinpath(filepath, filename)

    url = api_buildurl(endpoint, params, database, nothing)

    if verbose
        resp = HTTP.get(url)
    else
        resp = HTTP.get(url)
    end

    error = false
    message = nothing

    if resp.status != 200
        error = true
        body = JSON.parse(String(resp.body))
        message = get(body, "error", Dict()).get("message", nothing)
    end

    if !isnothing(message)
        println(message)
    else
        println("Downloaded file to " * destfile)
    end

    return (error = error, data = destfile)
end

"""
Delete epigraf data

Arguments:
- endpoint: The endpoint path
- params: Query parameters
- payload: The data posted to the endpoint
- database: The selected database
"""
function api_delete(endpoint, params = Dict(), payload = nothing, database = nothing)
    result = api_request(endpoint, params, payload, database, HTTP.delete)
    return result
end

"""
Patch data

Update records in the database using the API.
Existing records will be updated, missing records will be created.
The function supports uploading all data related to articles:
articles, sections, items, links, footnotes, properties, projects, users, types.
The IRI path in the ID column of the dataframe must contain the specific table name.

Arguments:
- data: A dataframe with the column `id`.
         Additional columns such as norm_data will be written to the record.
         The id must either be a a valid IRI path (e.g. properties/objecttypes/xxx)
         or an id prefixed by the table name (e.g. properties-12).
         Patching properties with prefixed ids requires a `type` column
         that contains the property type.
         Column names with table names as prefixes will be extracted, if wide is set to TRUE (default).
- db: The database name
- table: Optional: Check that the data only contains rows for a specific table
- type: Optional: Check that the data only contains rows with a specific type
- wide: Convert wide format to long format.
         If TRUE, column names prefixed with "properties", "items", "sections", "articles"
         and "projects" followed by a dot (e.g. "properties.id",
        "properties.lemma") will be extracted and patched as additional records.
"""
function api_patch(data, db, table = nothing, type = nothing, wide = true)
    if wide
        data = epi_wide_to_long(data)
    end

    if !epi_is_iripath(data.id, table, type) && !epi_is_id(data.id, table)
        throw(ErrorException("Data is empty or contains NA values."))
    end

    data = select(data, Not(["id"]))
    data = filter(row -> any(!ismissing, row), data)

    if (nrow(data) == 0) || (ncol(data) == 0)
        throw(ErrorException("Data is empty or contains NA values."))
    end

    if (ncol(data) == 1) && (names(data) == ["id"])
        throw(ErrorException("Skipped, the data only contains the ID column."))
    end

    println("Uploading " * string(nrow(data)) * " rows.")

    api_job_create("articles/import", nothing, db, Dict("data" => data))
end

"""
Send request to epigraf

Arguments:
- endpoint: The endpoint supporting job creation
- params: Query parameters
- payload: The data posted to the endpoint
- database: The selected database
- method: One of the HTTP functions (HTTP.post, HTTP.delete)
- encode: Payload encoding. Passed to the HTTP method function.
"""
function api_request(endpoint, params = Dict(), payload = nothing, database = nothing, method = HTTP.post, encode = "json")
    verbose = get(ENV, "epi_verbose", "false") == "true"
    server = get(ENV, "epi_apiserver", "")

    silent = get(ENV, "epi_silent", "false")
    if silent != "true"
        println("Posting data to " * server)
    end

    if !isLocalServer(server)
        confirmAction()
    end

    url = api_buildurl(endpoint, params, database)

    headers = Dict("Content-Type" => "application/json")
    if verbose
        resp = method(url, headers, JSON.json(payload))
    else
        resp = method(url, headers, JSON.json(payload))
    end

    body = JSON.parse(String(resp.body))

    error = false
    message = nothing

    if resp.status != 200
        error = true
        message = get(body, "error", Dict()).get("message", nothing)
    elseif get(body, "status", Dict()).get("success", true) != true
        error = true
        message = get(body, "status", Dict()).get("message", nothing)
    end

    if !isnothing(message)
        println(message)
    end

    return (error = error, data = body)
end

"""
Add the epi_tbl class and make it remember its source

Arguments:
- data: A tibble
- source: A named vector of source parameters, containing endpoint, parameters and database name
"""
function to_epitable(data, source = nothing)
    if !isnothing(source)
        data.source = source
    end

    id_cols = intersect(["database", "table", "row", "type", "norm_iri"], names(data))
    belongsto_id_cols = [col for col in names(data) if endswith(col, "id")]
    belongsto_name_cols = intersect(["project", "article", "section", "item", "property", "footnote"], names(data))
    state_cols = [col for col in names(data) if startswith(col, "created") || startswith(col, "modified")]
    content_cols = setdiff(names(data), vcat(id_cols, belongsto_id_cols, belongsto_name_cols, state_cols))

    data = select(data, vcat(id_cols, content_cols, belongsto_name_cols, belongsto_id_cols, state_cols))

    data.class = vcat("epi_tbl", setdiff(data.class, ["epi_tbl"]))
    return data
end

function isLocalServer(server)
    return startswith(server, "http://localhost") || startswith(server, "http://127.0.0.1")
end

function confirmAction()
    print("Are you sure you want to perform this action? (y/n): ")
    response = readline()
    if lowercase(response) != "y"
        throw(ErrorException("Action cancelled by user."))
    end
end

function epi_is_iripath(id, table, type)
    return true
end

function epi_is_id(id, table)
    return true
end

function epi_wide_to_long(data)
    return data
end
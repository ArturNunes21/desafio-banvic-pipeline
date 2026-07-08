with source_clientes as (

    select * from {{ source('banvic_raw', 'clientes') }}

)

, renamed_casted as (

    select
        cast(cod_cliente as integer) as id_cliente
        , cast(primeiro_nome as varchar) as primeiro_nome
        , cast(ultimo_nome as varchar) as sobrenome
        , lower(cast(email as varchar)) as email
        , cast(tipo_cliente as varchar) as tipo_cliente
        , cast(data_inclusao as timestamp) as data_inclusao_utc
        , cast(cpfcnpj as varchar) as documento_cliente
        , cast(data_nascimento as date) as data_nascimento
        , cast(endereco as varchar) as endereco_completo
        , cast(cep as varchar) as codigo_postal
    from source_clientes

)

select * from renamed_casted
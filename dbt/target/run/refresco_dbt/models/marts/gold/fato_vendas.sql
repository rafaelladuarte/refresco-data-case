
      
        
            delete from "gold"."gold"."fato_vendas"
            where (
                id_venda) in (
                select (id_venda)
                from "fato_vendas__dbt_tmp181040125667"
            );

        
    

    insert into "gold"."gold"."fato_vendas" ("sk_venda", "id_venda", "sk_cliente", "sk_produto", "sk_tempo", "sk_representante", "qtd_caixas", "preco_unitario", "desconto_pct", "receita_liquida", "custo_total", "margem_bruta", "tipo_lancamento", "status_financeiro", "data_emissao", "source_file", "ingested_at")
    (
        select "sk_venda", "id_venda", "sk_cliente", "sk_produto", "sk_tempo", "sk_representante", "qtd_caixas", "preco_unitario", "desconto_pct", "receita_liquida", "custo_total", "margem_bruta", "tipo_lancamento", "status_financeiro", "data_emissao", "source_file", "ingested_at"
        from "fato_vendas__dbt_tmp181040125667"
    )
  
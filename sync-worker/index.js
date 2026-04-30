import cron from 'node-cron';
import sql from 'mssql';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';

dotenv.config();

// 1. Configurar Supabase
const supabaseUrl = process.env.SUPABASE_URL || '';
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
const supabase = createClient(supabaseUrl, supabaseKey, {
  auth: { persistSession: false }
});

// 2. Configurar MSSQL
const sqlConfig = {
  user: process.env.MSSQL_USER,
  password: process.env.MSSQL_PASS,
  database: process.env.MSSQL_DB,
  server: process.env.MSSQL_HOST || 'freeway.inovareti.eti.br',
  port: parseInt(process.env.MSSQL_PORT || '52446'),
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000
  },
  options: {
    encrypt: true,
    trustServerCertificate: true
  }
};

// Flags de controle
let isSyncing = false;

async function syncProdutos(pool) {
  console.log('📦 Iniciando sync de Produtos...');
  try {
    const result = await pool.request().query(`
      SELECT 
        p.IdProduto, 
        p.CODPRODUTO as codProduto, 
        p.NOMEPRODUTO as nomeProduto, 
        p.Unid as unid, 
        p.PrecoVenda1 as precoVenda1, 
        p.PrecoVenda2 as precoVenda2, 
        p.CustoCompra2 as precoCusto2, 
        p.IdImagem1 as idImagem, 
        p.CodFabr as codFabr,
        CASE 
          WHEN pc.FatorConvUnid IS NOT NULL AND pc.FatorConvUnid != 0 
          THEN (1.0 / pc.FatorConvUnid) 
          ELSE 1 
        END AS QtdCaixa
      FROM Produtos p
      LEFT JOIN ProdutoConversao pc ON p.IdProduto = pc.IdProduto
    `);
    
    const produtos = result.recordset;
    console.log(`Encontrados ${produtos.length} produtos no MSSQL.`);

    // Lotes de 1000 para não estourar a memória
    const batchSize = 1000;
    for (let i = 0; i < produtos.length; i += batchSize) {
      const lote = produtos.slice(i, i + batchSize);
      
      const payload = lote.map(p => ({
        id_produto: p.IdProduto,
        cod_produto: p.codProduto,
        nome_produto: p.nomeProduto,
        unid: p.unid,
        preco_venda_1: p.precoVenda1,
        preco_venda_2: p.precoVenda2,
        preco_custo_2: p.precoCusto2,
        id_imagem: p.idImagem,
        cod_fabr: p.codFabr,
        qtd_caixa: p.QtdCaixa,
        updated_at: new Date().toISOString()
      }));

      const { error } = await supabase.from('produtos').upsert(payload, { onConflict: 'id_produto' });
      if (error) throw error;
      
      console.log(`✅ Lote produtos ${i + lote.length}/${produtos.length} sincronizado.`);
    }
  } catch (error) {
    console.error('❌ Erro no sync de Produtos:', error.message);
  }
}

async function syncEstoque(pool) {
  console.log('📦 Iniciando sync de Estoque...');
  try {
    const result = await pool.request().query(`
      SELECT 
        e.IdEstoque, 
        e.CodFilial, 
        e.CodLocal, 
        e.IdProduto, 
        e.EstoqueMinimo, 
        e.SdoAtual, 
        e.LocalArmazenamento
      FROM Estoque e
      WHERE e.CodLocal IN ('00', '10', '20')
    `);
    
    const estoque = result.recordset;
    console.log(`Encontrados ${estoque.length} registros de estoque no MSSQL.`);

    const batchSize = 1000;
    for (let i = 0; i < estoque.length; i += batchSize) {
      const lote = estoque.slice(i, i + batchSize);
      
      const payload = lote.map(e => ({
        id_estoque: e.IdEstoque,
        cod_filial: e.CodFilial,
        cod_local: e.CodLocal,
        id_produto: e.IdProduto,
        sdo_atual: e.SdoAtual,
        estoque_minimo: e.EstoqueMinimo,
        local_armazenamento: e.LocalArmazenamento,
        updated_at: new Date().toISOString()
      }));

      const { error } = await supabase.from('estoque').upsert(payload, { onConflict: 'id_estoque' });
      if (error) throw error;
      
      console.log(`✅ Lote estoque ${i + lote.length}/${estoque.length} sincronizado.`);
    }
  } catch (error) {
    console.error('❌ Erro no sync de Estoque:', error.message);
  }
}

async function syncCodigoBarras(pool) {
  console.log('📦 Iniciando sync de Códigos de Barras...');
  try {
    const result = await pool.request().query(`
      SELECT IdProduto, CodigoBarras 
      FROM codigoBarras
    `);
    
    const codigos = result.recordset;
    console.log(`Encontrados ${codigos.length} códigos de barras no MSSQL.`);

    const batchSize = 1000;
    for (let i = 0; i < codigos.length; i += batchSize) {
      const lote = codigos.slice(i, i + batchSize);
      
      const payload = lote.map(c => ({
        id_produto: c.IdProduto,
        codigo_barras: c.CodigoBarras,
        updated_at: new Date().toISOString()
      }));

      const { error } = await supabase.from('codigo_barras').upsert(payload, { onConflict: 'codigo_barras' });
      if (error) throw error;
      
      console.log(`✅ Lote códigos ${i + lote.length}/${codigos.length} sincronizado.`);
    }
  } catch (error) {
    console.error('❌ Erro no sync de Códigos de Barras:', error.message);
  }
}

async function syncImagens(pool) {
  console.log('📦 Iniciando sync de Imagens...');
  try {
    const result = await pool.request().query(`
      SELECT IdImagem, imagem 
      FROM imagem 
      WHERE imagem IS NOT NULL
    `);
    
    const imagens = result.recordset;
    console.log(`Encontradas ${imagens.length} imagens no MSSQL.`);

    let salvos = 0;
    let pulados = 0;
    let erros = 0;

    for (const img of imagens) {
      try {
        if (!img.imagem) continue;
        
        const fileName = `${img.IdImagem}.jpg`;
        
        // Verifica se a imagem já existe no bucket (Evitar upload duplo)
        const { data: existingFiles } = await supabase.storage.from('produtos-imagens').list('', {
          search: fileName
        });

        if (existingFiles && existingFiles.length > 0 && existingFiles[0].name === fileName) {
          pulados++;
          continue;
        }

        // Upload do Buffer direto pro Supabase
        const { data, error } = await supabase.storage
          .from('produtos-imagens')
          .upload(fileName, img.imagem, {
            contentType: 'image/jpeg',
            upsert: false
          });

        if (error) throw error;
        salvos++;
        
        if (salvos % 50 === 0) console.log(`🚀 Já fizemos upload de ${salvos} fotos novas...`);
        
      } catch (err) {
        erros++;
        console.error(`Erro na imagem ${img.IdImagem}:`, err.message);
      }
    }
    
    console.log(`✅ Sync de Imagens Finalizado: ${salvos} Novos Uploads | ${pulados} Mantidos | ${erros} Erros`);
  } catch (error) {
    console.error('❌ Erro no sync de Imagens:', error.message);
  }
}

async function runSync() {
  if (isSyncing) {
    console.log('⚠️ Sincronização anterior ainda em andamento. Pulando este ciclo.');
    return;
  }

  isSyncing = true;
  console.log('\n=======================================');
  console.log(`🚀 Iniciando Ciclo de Sincronização: ${new Date().toLocaleString()}`);
  
  let pool;
  try {
    pool = await sql.connect(sqlConfig);
    
    // Roda os processos em sequência para não sobrecarregar a memória do worker (50MB)
    await syncProdutos(pool);
    await syncEstoque(pool);
    await syncCodigoBarras(pool);
    await syncImagens(pool); // Agora sim faz o upload das fotos!

    console.log(`🎉 Ciclo de Sincronização Concluído: ${new Date().toLocaleString()}`);
    console.log('=======================================\n');
  } catch (error) {
    console.error('❌ Erro Crítico na Conexão com MSSQL:', error.message);
  } finally {
    if (pool) pool.close();
    isSyncing = false;
  }
}

// 3. Iniciar o Cron Job (Roda a cada 30 minutos)
cron.schedule('*/30 * * * *', () => {
  runSync();
});

console.log('🟢 Worker de Sincronização Iniciado (Aguardando ciclos do Cron...)');
// Roda a primeira vez ao ligar o container
runSync();

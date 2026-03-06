# [Ref: 01_语义分类器_实践] [Ref: global_const.deployable_units.module_a]
# Module A 语义分类器镜像：连接 L1/L2 由环境变量注入，不写死密钥

FROM python:3.11-slim

WORKDIR /app

# 系统依赖（psycopg2、可选 make 供镜像内 make test）
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    make \
    && rm -rf /var/lib/apt/lists/*

# Module A 运行时依赖（YAML、L1/L2、Proto）
COPY requirements-module-a.txt ./
RUN pip install --no-cache-dir -r requirements-module-a.txt

# 项目代码与配置（规则不硬编码，从 config/classifier_rules.yaml 加载）
COPY . .

# 镜像内验证：可执行 make test-classifier 或 python -m diting.classifier.run
# 默认入口：执行一次全量分类（K8s 下由 Secret 注入 TIMESCALE_DSN、PG_L2_DSN）
# RUN_LOOP=1 时每 RUN_LOOP_INTERVAL_SEC 秒执行一次，供长期运行 Deployment
CMD ["python", "-m", "diting.classifier.run"]

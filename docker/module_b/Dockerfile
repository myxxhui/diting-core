# [Ref: 02_量化扫描引擎_实践] [Ref: global_const.deployable_units.module_b]
# Module B 量化扫描引擎镜像：TA-Lib 需先安装系统层 C 库；L1/L2 由环境变量注入

FROM python:3.8-slim

WORKDIR /app

# 系统依赖：构建 ta-lib C 库、psycopg2、镜像内 make test
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    libpq5 \
    make \
    && rm -rf /var/lib/apt/lists/*

# 下载并编译安装 ta-lib 0.4.0（Python TA-Lib 依赖 -lta-lib）
ARG TALIB_VERSION=0.4.0
RUN wget -q "https://sourceforge.net/projects/ta-lib/files/ta-lib/${TALIB_VERSION}/ta-lib-${TALIB_VERSION}-src.tar.gz/download" -O /tmp/ta-lib-src.tar.gz \
    && tar -xzf /tmp/ta-lib-src.tar.gz -C /tmp \
    && cd /tmp/ta-lib \
    && ./configure --prefix=/usr \
    && make -C src -j$(nproc) \
    && make -C src install \
    && ldconfig \
    && cd / \
    && rm -rf /tmp/ta-lib /tmp/ta-lib-src.tar.gz
# Python 包查找 -lta-lib；ta-lib 安装到 /usr/lib64 或 /usr/lib，创建兼容符号链接并确保链接器可找到
RUN for d in /usr/lib64 /usr/lib/x86_64-linux-gnu /usr/lib; do \
    if [ -f "$d/libta_lib.so" ] || [ -f "$d/libta_lib.so.0" ]; then \
      (cd "$d" && ln -sf libta_lib.so libta-lib.so 2>/dev/null) || (cd "$d" && ln -sf libta_lib.so.0 libta-lib.so); \
      break; fi; done
ENV LIBRARY_PATH="/usr/lib64:/usr/lib/x86_64-linux-gnu:/usr/lib:${LIBRARY_PATH:-}"
ENV LD_LIBRARY_PATH="/usr/lib64:/usr/lib/x86_64-linux-gnu:/usr/lib:${LD_LIBRARY_PATH:-}"

# Module B 运行时依赖（numpy、PyYAML、psycopg2、TA-Lib）；单测用 pytest
COPY requirements-scanner.txt ./
RUN pip install --no-cache-dir -r requirements-scanner.txt && pip install --no-cache-dir pytest

# 项目代码与配置（策略池 YAML 从 config/scanner_rules.yaml 加载）
COPY . .

# 镜像内验证：make test-scanner 或直接运行扫描脚本
# 默认入口：执行一次扫描（K8s 下由 Secret 注入 TIMESCALE_DSN、PG_L2_DSN）
CMD ["python", "scripts/run_module_b_local.py"]

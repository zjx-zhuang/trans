# 使用多阶段构建优化镜像大小
FROM python:3.11-slim as builder

# 安装构建依赖
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 复制依赖文件
COPY requirements.txt .

# 创建虚拟环境并安装依赖
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 生产阶段
FROM python:3.11-slim

# 安装运行时依赖 (curl 用于健康检查)
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 创建非root用户
RUN groupadd -r appuser && useradd -r -g appuser appuser

# 复制虚拟环境
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# 设置工作目录
WORKDIR /app

# 复制应用代码
COPY src/ ./src/
COPY static/ ./static/
COPY tests/ ./tests/

# 设置正确的权限
RUN chown -R appuser:appuser /app

# 切换到非root用户
USER appuser

# 设置环境变量默认值
ENV PYTHONPATH=/app
ENV LOG_LEVEL=INFO
ENV LLM_PROVIDER=gemini
ENV BQ_VALIDATION_MODE=dry_run
ENV SQL_CHUNKING_MODE=auto
ENV MAX_SQL_LENGTH=8000
ENV MAX_SQL_LINES=200

# [修改 1] 健康检查也要使用动态端口，或者在 Cloud Run 中其实可以忽略这一行
# 这里为了本地测试方便，改为引用环境变量
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f https://localhost:${PORT:-8000}/health || exit 1

# 暴露端口 (仅作文档用途)
EXPOSE 8000

# [修改 2 - 关键修复] 使用 Shell 模式启动，以便读取 $PORT 环境变量
# 如果不这样改，Cloud Run 部署必挂
CMD exec uvicorn src.main:app --host 0.0.0.0 --port ${PORT:-8000}
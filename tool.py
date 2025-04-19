from typing import Any
import requests

from dify_plugin import ToolProvider
from dify_plugin.errors.tool import ToolProviderCredentialValidationError
from query import BigQueryTool

class MCPBigQueryProvider(ToolProvider):
    def _validate_credentials(self, credentials: dict[str, Any]) -> None:
        try:
            # 자격 증명 검증을 위한 간단한 쿼리 실행
            for _ in BigQueryTool.from_credentials(credentials).invoke(
                tool_parameters={"sql": "SELECT 1"},
            ):
                pass
        except Exception as e:
            raise ToolProviderCredentialValidationError(str(e))
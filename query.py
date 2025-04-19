from collections.abc import Generator
from typing import Any
import requests

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage

class BigQueryTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        # 필수 파라미터 검증
        if "sql" not in tool_parameters:
            raise ValueError("SQL query is required")
            
        # 환경 설정
        mcp_api_url = self.runtime.credentials.get("mcp_api_url")
        mcp_api_key = self.runtime.credentials.get("mcp_api_key")
        
        # 기본값 설정
        project_id = "pj-lge-gmc-ga4-global-456308"
        location = "asia-northeast3"
        
        # API 요청 데이터 구성
        request_data = {
            "operation": "executeTool",
            "toolName": "query",
            "toolParameters": {
                "sql": tool_parameters["sql"],
                "projectId": project_id,
                "location": location,
                "maximumBytesBilled": tool_parameters.get("maximumBytesBilled")
            }
        }
        
        # MCP API 호출
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {mcp_api_key}" if mcp_api_key else None
        }
        
        try:
            response = requests.post(mcp_api_url, json=request_data, headers=headers)
            response.raise_for_status()
            
            # 결과 반환
            yield self.create_json_message(response.json())
        except requests.exceptions.RequestException as e:
            # API 요청 오류 처리
            error_message = f"MCP BigQuery API Error: {str(e)}"
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_details = e.response.json()
                    error_message += f" - {error_details.get('message', '')}"
                except:
                    pass
            raise ValueError(error_message)
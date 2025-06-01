import asyncio
import ssl
import io
import logging

from micropython import const

UPLOAD_BUFFER_SIZE = const(1024 * 40)
DOWNLOAD_BUFFER_SIZE = const(1024 * 100)

class AsyncHttpResponse:
    status_code: int
    status_message: str
    headers: dict[str, str]
    content_length: int | None
    received_bytes: int

    def __init__(self, status_code: int, status_message: str, headers: dict[str, str], reader: asyncio.StreamReader, logger: logging.Logger) -> None:
        self._reader = reader
        self.status_code = status_code
        self.status_message = status_message
        self.headers = headers
        self.received_bytes = 0
        self._logger = logger
        self.content_length = int(headers['content-length']) if 'content-length' in headers else None

    async def close(self) -> None:
        if self._reader is not None:
            self._reader.close()
            await self._reader.wait_closed()
            self._reader = None

    async def readline(self) -> bytes:
        if self._reader is not None:
            return await self._reader.readline()
        else:
            raise ValueError('Response closed')

    async def readinto(self, buffer: bytearray | memoryview) -> int:
        if self._reader is not None:
            return await self._reader.readinto(buffer)
        else:
            raise ValueError('Response closed')

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        if self.content_length is None:
            raise AsyncHttpException('No content length header found')
        if self.received_bytes == self.content_length:
            await self.close()
            raise StopAsyncIteration
        buffer = bytearray()
        while self._reader is not None and self.received_bytes < self.content_length and len(buffer) < DOWNLOAD_BUFFER_SIZE:
            self._logger.debug('Request more (%d B in buffer)', len(buffer))
            data = await self._reader.read(DOWNLOAD_BUFFER_SIZE)
            length = len(data)
            self._logger.debug('Received %d B', length)
            if length == 0:
                break
            self.received_bytes += length
            buffer.extend(data)
        self._logger.debug('Return chunk (%d B)', len(buffer))
        return buffer

class AsyncHttpException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class AsyncHttp:
    def __init__(self, asyncio=asyncio, logging=logging) -> None:
        self._asyncio = asyncio
        self._logger = logging.getLogger('AsyncHttp')

    async def get(self, host: str, path: str, headers: dict={}, ssl_ctx: ssl.SSLContext | None=None) -> AsyncHttpResponse:
        reader, writer = await self._open('GET', host, path, headers=headers, ssl_ctx=ssl_ctx)
        await writer.drain()
        writer.close()
        status_code, status_message = await self._read_status(reader)
        headers = await self._read_headers(reader)
        return AsyncHttpResponse(status_code, status_message, headers, reader, self._logger)

    async def put(self, host: str, path: str, body: io.BufferedReader, body_size: int, headers: dict={}, ssl_ctx: ssl.SSLContext | None=None) -> AsyncHttpResponse:
        upload_headers = headers.copy()
        upload_headers['Content-Length'] = body_size
        reader, writer = await self._open('PUT', host, path, headers=upload_headers, ssl_ctx=ssl_ctx)
        buffer = memoryview(bytearray(UPLOAD_BUFFER_SIZE))
        bytes_written = 0
        while True:
            bytes_read = body.readinto(buffer)
            if bytes_read == 0:
                break
            bytes_written += bytes_read
            self._logger.debug('Sending %d B (%d of %d B)', bytes_read, bytes_written, body_size)
            writer.write(buffer[0:bytes_read])
            await writer.drain()
        writer.close()
        status_code, status_message = await self._read_status(reader)
        headers = await self._read_headers(reader)
        return AsyncHttpResponse(status_code, status_message, headers, reader, self._logger)

    async def _open(self, method: str, host: str, path: str, headers: dict={}, ssl_ctx: ssl.SSLContext | None=None) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        port = 80 if ssl_ctx is None else 443
        reader, writer = await self._asyncio.open_connection(host, port, ssl=ssl_ctx)
        writer.write(f'{method} {path} HTTP/1.0\r\n')
        for header, value in headers.items():
            writer.write(f'{header}: {value}\r\n')
        writer.write(f'Host: {host}\r\nConnection: close\r\n')
        writer.write('\r\n')
        self._logger.debug('Requesting %s http://%s%s', method, host, path)
        return (reader, writer)

    async def _read_status(self, reader: asyncio.StreamReader) -> tuple[int, str]:
        while True:
            status_line = await reader.readline()
            if status_line is None or len(status_line) == 0:
                reader.close()
                raise AsyncHttpException('Unexpected end of response')
            if len(status_line) > 7 and status_line[8] == 32:
                status_code = int(status_line[9:12])
                status_message = status_line[13:-2].decode('ascii')
                return (status_code, status_message)
            else:
                self._logger.warning('Could not parse status line from "%s"', status_line)
                raise AsyncHttpException('Could not parse status')

    async def _read_headers(self, reader: asyncio.StreamReader) -> dict[str, str]:
        headers = {}
        while True:
            line = await reader.readline()
            if line is None or len(line) == 0:
                reader.close()
                raise AsyncHttpException('Unexpected end of response')
            elif line == b'\r\n':
                return headers
            else:
                try:
                    colon_index = line.index(b':')
                    header_name = line[0:colon_index].decode('ascii')
                    if line[colon_index + 1] == 32:
                        header_value = line[(colon_index + 2):-2]
                    else:
                        header_value = line[(colon_index + 1):-1]
                    headers[header_name.lower()] = header_value.decode('ascii')
                except ValueError as e:
                    reader.close()
                    self._logger.warning('Could not parse header from "%s"', line)
                    raise AsyncHttpException('Could not parse header', e)

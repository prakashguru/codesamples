using Dropbox.Api;
using Dropbox.Api.Files;
using Dropbox.Api.Files.Routes;
using Dropbox.Api.Sharing.Routes;
using Dropbox.Api.Users.Routes;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
namespace SimpleTest
{
    /// <summary>
    /// The default DropboxClient has performance issues with upload. 
    /// This class provides performance optimized upload functionality.
    /// </summary>
    internal sealed class ExtendedDropboxClient : IDisposable
    {
        #region Private Data Members

        DropboxClient _dropboxClient = null;
        OAuth2Response _authenticationDetail;
        DropboxClientConfig _clientConfig;

        private const string DropboxApiArgHeader = "Dropbox-API-Arg";
        private const string DropboxApiResultHeader = "Dropbox-API-Result";
        private const int UploadBufferSize =  1048576; //1 MB
        private const int UploadChunkSize = 125829120; //120 MB 
        private const string ContentEndpoint = "https://content.dropboxapi.com/2/";
        #endregion

        #region Initializers & Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="T:Dropbox.Api.DropboxClient"/> class.
        /// </summary>
        /// <param name="authenticationDetail">The oauth2 response from the dropbox requests.</param>
        public ExtendedDropboxClient(OAuth2Response authenticationDetail)
            : this(authenticationDetail, new DropboxClientConfig())
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:Dropbox.Api.DropboxClient"/> class.
        /// </summary>
        /// <param name="oauth2AccessToken">The oauth2 access token for making client requests.</param>
        /// <param name="config">The <see cref="DropboxClientConfig"/>.</param>
        public ExtendedDropboxClient(OAuth2Response authenticationDetail, DropboxClientConfig config)
        {
            if (string.IsNullOrWhiteSpace(authenticationDetail.AccessToken))
                throw new ArgumentNullException("authenticationDetail.AccessToken");

            if (config == null)
                throw new ArgumentNullException("config");

            if (config.HttpClient == null)
                config.HttpClient = new HttpClient();

            _authenticationDetail = authenticationDetail;
            _clientConfig = config;

            _dropboxClient = new DropboxClient(authenticationDetail.AccessToken, config);
        }


        /// <summary>
        /// Disposes the ExtendedDropboxClient instance.
        /// </summary>
        public void Dispose()
        {
            if (_dropboxClient != null)
            {
                _dropboxClient.Dispose();
                _dropboxClient = null;
            }

            if (_clientConfig.HttpClient != null)
            {
                _clientConfig.HttpClient.Dispose();
                _clientConfig.HttpClient = null;
            }
        }

        #endregion

        #region DropboxClient Memebers

        /// <summary>
        /// <para>Gets the Files routes.</para>
        /// </summary>
        public FilesRoutes Files { get { return _dropboxClient.Files; } }

        /// <summary>
        /// <para>Gets the Sharing routes.</para>
        /// </summary>
        public SharingRoutes Sharing { get { return _dropboxClient.Sharing; } }

        /// <summary>
        /// <para>Gets the Users routes.</para>
        /// </summary>
        public UsersRoutes Users { get { return _dropboxClient.Users; } }

        #endregion

        #region ExtendedDropboxClient Members

        public async Task<string> UploadAsync(CommitInfo commitInfo, Stream streamToUpload)
        {
            if (commitInfo == null)
                throw new ArgumentNullException("commitInfo");

            if (string.IsNullOrWhiteSpace(commitInfo.Path))
                throw new ArgumentOutOfRangeException("commitInfo.Path", "The file path is required.");

            if (streamToUpload == null)
                throw new ArgumentNullException("streamToUpload");

            if (!streamToUpload.CanRead)
                throw new ArgumentOutOfRangeException("streamToUpload", "The stream is not readable.");

            //dropbox supports at max 150 MB per request for upload. We have to send the file content in batches to dropbox.
            //Uploading to dropbox is a three step process. 
            // 1. Initiate the session https://www.dropbox.com/developers/documentation/http/documentation#files-upload_session-start
            // 2. Upload the content in chunks https://www.dropbox.com/developers/documentation/http/documentation#files-upload_session-append
            // 3. Finish the upload session https://www.dropbox.com/developers/documentation/http/documentation#files-upload_session-finish
            // The step 2 is optional. If the file is less then 150 MB the step 2 can be skipped.

            string uploadResponse = null; 
           
            using (var chunkedWriter = new ChunkedStreamWriter(streamToUpload))
            {
                var httpRequest = GetHttpRequestMessage(ExtendedDropboxClient.ContentEndpoint + "files/upload_session/start");

                httpRequest.Content = new PushStreamContent(new Func<Stream, HttpContent, TransportContext, Task>(chunkedWriter.UploadOnStreamAvailable), "application/octet-stream");

                var response = await _clientConfig.HttpClient.SendAsync(httpRequest, HttpCompletionOption.ResponseContentRead).ConfigureAwait(false);

                if (!response.IsSuccessStatusCode)
                {
                    throw new ApplicationException(await response.Content.ReadAsStringAsync());
                }

                var sessionData = await response.Content.ReadAsAsync<UploadSessionCursor>();
                sessionData.Offset = chunkedWriter.Offset;

                while (chunkedWriter.HasMoreContent)
                {
                    httpRequest = GetHttpRequestMessage(ExtendedDropboxClient.ContentEndpoint + "files/upload_session/append");
                    httpRequest.Headers.Add(ExtendedDropboxClient.DropboxApiArgHeader, JsonConvert.SerializeObject(sessionData));

                    httpRequest.Content = new PushStreamContent(new Func<Stream, HttpContent, TransportContext, Task>(chunkedWriter.UploadOnStreamAvailable), "application/octet-stream");

                    response = await _clientConfig.HttpClient.SendAsync(httpRequest, HttpCompletionOption.ResponseContentRead).ConfigureAwait(false);
                    //This line is required otherwise _clientConfig.HttpClient will result in deadlock on it's next call.
                    var result = await response.Content.ReadAsStringAsync();

                    if (!response.IsSuccessStatusCode)
                    {
                        throw new ApplicationException(result);
                    }

                    sessionData.Offset = chunkedWriter.Offset;
                }

                var finishData = new UploadSessionFinish()
                {
                    Cursor = sessionData,
                    CommitInfo = new CommitInfo()
                    {
                        AutoRename = commitInfo.AutoRename,
                        Mode = "add",
                        Mute = commitInfo.Mute,
                        Path = commitInfo.Path
                    }
                };

                httpRequest = GetHttpRequestMessage(ExtendedDropboxClient.ContentEndpoint + "files/upload_session/finish");
                httpRequest.Headers.Add(ExtendedDropboxClient.DropboxApiArgHeader, JsonConvert.SerializeObject(finishData));
                httpRequest.Content = new PushStreamContent(new Func<Stream, HttpContent, TransportContext, Task>(chunkedWriter.UploadOnStreamAvailable), "application/octet-stream");
                response = await _clientConfig.HttpClient.SendAsync(httpRequest, HttpCompletionOption.ResponseContentRead).ConfigureAwait(false);

                //This line is required otherwise _clientConfig.HttpClient will result in deadlock on it's next call.
                uploadResponse = await response.Content.ReadAsStringAsync();

                if (!response.IsSuccessStatusCode)
                {
                    throw new ApplicationException(uploadResponse);
                }
            }

            return uploadResponse;
        }

        private HttpRequestMessage GetHttpRequestMessage(string url)
        {
            var httpReqMessage = new HttpRequestMessage(HttpMethod.Post, url);
            httpReqMessage.Headers.Authorization = new AuthenticationHeaderValue(_authenticationDetail.TokenType, _authenticationDetail.AccessToken);
            httpReqMessage.Headers.TransferEncodingChunked = true;
            httpReqMessage.Headers.TryAddWithoutValidation("User-Agent", _clientConfig.UserAgent);

            return httpReqMessage;
        }

        #endregion

        private sealed class ChunkedStreamWriter : IDisposable
        {
            #region Data Members

            Stream _streamToUpload;
            byte[] _buffer;

            #endregion

            #region Initializers

            public ChunkedStreamWriter(Stream streamToUpload)
            {
                if (streamToUpload == null) throw new ArgumentNullException("streamToUpload");

                _streamToUpload = streamToUpload;
                _buffer = new byte[ExtendedDropboxClient.UploadBufferSize];

                Offset = 0;
            }

            #endregion

            #region Properties

            /// <summary>
            /// Flag Indicates whether the stream to upload has more data or not.
            /// </summary>
            public bool HasMoreContent { get; set; }

            /// <summary>
            /// The offset of the upload stream.
            /// </summary>
            public ulong Offset { get; set; }

            #endregion

            #region Members

            public async Task UploadOnStreamAvailable(Stream uploadStream, HttpContent httpContent, TransportContext transportContext)
            {
                HasMoreContent = false;
                int totalBytesWrittenOnThisSession = 0;
                int readedBytes = 0;

                using (uploadStream)
                {
                    while ((readedBytes = _streamToUpload.Read(_buffer, 0, ExtendedDropboxClient.UploadBufferSize)) > 0)
                    {
                        await uploadStream.WriteAsync(_buffer, 0, readedBytes);

                        totalBytesWrittenOnThisSession += readedBytes;

                        if (totalBytesWrittenOnThisSession >= ExtendedDropboxClient.UploadChunkSize) //100 MB. Write upto 100 MB
                        {
                            HasMoreContent = true;
                            break;
                        }
                    }

                    if (readedBytes == 0)
                        HasMoreContent = false;
                }

                Offset += (uint)totalBytesWrittenOnThisSession;
            }

            public void Dispose()
            {
                _streamToUpload.Dispose();
            }

            #endregion
        }

        internal class UploadSessionFinish
        {
            [JsonProperty("cursor")]
            public UploadSessionCursor Cursor { get; set; }
            [JsonProperty("commit")]
            public CommitInfo CommitInfo { get; set; }
        }

        internal class UploadSessionCursor
        {
            [JsonProperty("session_id")]
            public string SessionId { get; set; }

            [JsonProperty("offset")]
            public ulong Offset { get; set; }
        }

        internal class CommitInfo
        {
            [JsonProperty("path")]
            public string Path { get; set; }

            [JsonProperty("mode")]
            public string Mode { get; set; }

            [JsonProperty("autorename")]
            public bool AutoRename { get; set; }

            [JsonProperty("mute")]
            public bool Mute { get; set; }
        }
    }
}

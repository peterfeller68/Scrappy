using Microsoft.Extensions.Logging;
using OpenAI.Chat;
using ScrappyFunctionApp.Data;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ScrappyFunctionApp
{
    class OpenAIClass
    {
        const string OpenAIKey = "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";
        string _connStr;
        ChatClient openAIclient;
        ILogger _log;

        //Models
        // gpt-4.1
        public OpenAIClass(ILogger log, string model= "gpt-4.1")
        {
            openAIclient = new(model: model, apiKey: OpenAIKey);
            _log = log;
        }

        public async Task<string> CompareTwoStrings(SitesClass site, string curr, string prev)
        {
            if (_log != null) _log.LogInformation($"{site.Label}, Calling GPT4 to find deltas");
            List<ChatMessage> messages = new List<ChatMessage>();
            messages.Add(new UserChatMessage(curr));
            messages.Add(new UserChatMessage(prev));

            string prompt = site.GptPrompt;
            if (string.IsNullOrEmpty(prompt))
            {
                prompt = "Please return the differences between the two strings.";
            }
            messages.Add(prompt);

            ChatCompletion response = await openAIclient.CompleteChatAsync(messages);
            return response.Content[0].Text;
        }

    }
}

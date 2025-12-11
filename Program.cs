using System.Collections.Concurrent;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using SIPSorcery.Net;

Console.WriteLine("=== WebRTC P2P Group Chat ===");
Console.Write("Enter your username: ");
string username = Console.ReadLine() switch
{
    { } name when !string.IsNullOrWhiteSpace(name) => name,
    _ => $"User{Random.Shared.Next(1000, 9999)}"
};

const string MulticastAddressIpv4 = "239.255.42.99";
const string MulticastAddressIpv6 = "ff02::1";
const int MulticastPort = 42099;

string myPeerId = Guid.NewGuid().ToString()[..8];
bool isRunning = true;
bool isHub = false;
string? hubPeerId = null;
ConcurrentDictionary<string, PeerConnection> peers = [];

UdpClient? udpClientIpv4 = null;
UdpClient? udpClientIpv6 = null;
IPEndPoint? multicastEndpointIpv4 = null;
IPEndPoint? multicastEndpointIpv6 = null;

Console.WriteLine($"[Info] My ID: {myPeerId}");

NetworkInterface? selectedNetworkInterface = SelectNetworkInterface();
if (selectedNetworkInterface is null)
{
    Console.WriteLine("[Error] No suitable network interface found");
    return;
}

Console.WriteLine($"[Info] Using network interface: {selectedNetworkInterface.Name}");
Console.WriteLine($"[Info] Interface type: {selectedNetworkInterface.NetworkInterfaceType}");
Console.WriteLine($"[Info] Status: {selectedNetworkInterface.OperationalStatus}");

if (GetInterfaceAddress(selectedNetworkInterface, AddressFamily.InterNetwork) is { } ipv4Address)
{
    try
    {
        Console.WriteLine($"[Info] IPv4 address: {ipv4Address}");
        udpClientIpv4 = new UdpClient(AddressFamily.InterNetwork);
        udpClientIpv4.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        udpClientIpv4.Client.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastTimeToLive, 32);
        udpClientIpv4.Client.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastLoopback, true);
        udpClientIpv4.Client.Bind(new IPEndPoint(IPAddress.Any, MulticastPort));

        MulticastOption multicastOptionIpv4 = new(IPAddress.Parse(MulticastAddressIpv4), ipv4Address);
        udpClientIpv4.Client.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.AddMembership, multicastOptionIpv4);
        multicastEndpointIpv4 = new IPEndPoint(IPAddress.Parse(MulticastAddressIpv4), MulticastPort);
        Console.WriteLine($"[Info] IPv4 multicast configured on {MulticastAddressIpv4}:{MulticastPort}");
    }
    catch (Exception exception)
    {
        Console.WriteLine($"[Warning] IPv4 setup failed: {exception.Message}");
        udpClientIpv4?.Dispose();
        udpClientIpv4 = null;
    }
}

if (GetInterfaceAddress(selectedNetworkInterface, AddressFamily.InterNetworkV6) is { } ipv6Address)
{
    try
    {
        Console.WriteLine($"[Info] IPv6 address: {ipv6Address}");
        udpClientIpv6 = new UdpClient(AddressFamily.InterNetworkV6);
        udpClientIpv6.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        udpClientIpv6.Client.SetSocketOption(SocketOptionLevel.IPv6, SocketOptionName.MulticastTimeToLive, 255);
        udpClientIpv6.Client.SetSocketOption(SocketOptionLevel.IPv6, SocketOptionName.MulticastLoopback, true);
        udpClientIpv6.Client.Bind(new IPEndPoint(IPAddress.IPv6Any, MulticastPort));

        int networkInterfaceIndex = selectedNetworkInterface.GetIPProperties().GetIPv6Properties().Index;
        IPv6MulticastOption multicastOptionIpv6 = new(IPAddress.Parse(MulticastAddressIpv6), networkInterfaceIndex);
        udpClientIpv6.Client.SetSocketOption(SocketOptionLevel.IPv6, SocketOptionName.AddMembership, multicastOptionIpv6);
        multicastEndpointIpv6 = new IPEndPoint(IPAddress.Parse(MulticastAddressIpv6), MulticastPort);
        Console.WriteLine($"[Info] IPv6 multicast configured on {MulticastAddressIpv6}:{MulticastPort}");
    }
    catch (Exception exception)
    {
        Console.WriteLine($"[Warning] IPv6 setup failed: {exception.Message}");
        udpClientIpv6?.Dispose();
        udpClientIpv6 = null;
    }
}

if (udpClientIpv4 is null && udpClientIpv6 is null)
{
    Console.WriteLine("[Error] Failed to setup both IPv4 and IPv6 multicast");
    return;
}

DateTime hubDiscoveryStartTime = DateTime.UtcNow;
bool foundHub = false;

Console.WriteLine("[Discovery] Scanning for hub (5 seconds)...");

while ((DateTime.UtcNow - hubDiscoveryStartTime).TotalSeconds < 5 && !foundHub)
{
    try
    {
        if (udpClientIpv4?.Available > 0)
        {
            UdpReceiveResult receiveResult = await udpClientIpv4.ReceiveAsync();
            string receivedMessage = Encoding.UTF8.GetString(receiveResult.Buffer);

            try
            {
                if (JsonSerializer.Deserialize<HubAnnouncement>(receivedMessage) is { Type: "hub" } hubAnnouncement)
                {
                    (hubPeerId, foundHub) = (hubAnnouncement.HubId, true);
                    Console.WriteLine($"[Discovery] Found hub: {hubAnnouncement.HubUsername} ({hubPeerId})");
                    break;
                }
            }
            catch (JsonException)
            {
            }
        }

        if (udpClientIpv6?.Available > 0)
        {
            UdpReceiveResult receiveResult = await udpClientIpv6.ReceiveAsync();
            string receivedMessage = Encoding.UTF8.GetString(receiveResult.Buffer);

            try
            {
                if (JsonSerializer.Deserialize<HubAnnouncement>(receivedMessage) is { Type: "hub" } hubAnnouncement)
                {
                    (hubPeerId, foundHub) = (hubAnnouncement.HubId, true);
                    Console.WriteLine($"[Discovery] Found hub: {hubAnnouncement.HubUsername} ({hubPeerId})");
                    break;
                }
            }
            catch (JsonException)
            {
            }
        }

        await Task.Delay(100);
    }
    catch (Exception exception)
    {
        Console.WriteLine($"[Warning] Error during hub discovery: {exception.Message}");
    }
}

Console.WriteLine($"[Discovery] Scan complete. Found hub: {foundHub}");

if (!foundHub)
{
    (isHub, hubPeerId) = (true, myPeerId);
    Console.WriteLine("[Mode] I am the HUB - all others will connect to me");
}
else
{
    Console.WriteLine("[Mode] I am a CLIENT - connecting to hub");
}

await (isHub switch
{
    true => RunAsHub(udpClientIpv4, udpClientIpv6, multicastEndpointIpv4, multicastEndpointIpv6, username, myPeerId, isRunning, peers),
    false => RunAsClient(udpClientIpv4, udpClientIpv6, multicastEndpointIpv4, multicastEndpointIpv6, username, myPeerId, isRunning, hubPeerId, peers)
});

static NetworkInterface? SelectNetworkInterface()
{
    try
    {
        Console.WriteLine("[Info] Detecting available network interfaces...");
        NetworkInterface[] allNetworkInterfaces = NetworkInterface.GetAllNetworkInterfaces();
        Console.WriteLine($"[Info] Found {allNetworkInterfaces.Length} network interface(s)\n");

        List<NetworkInterface> activeNetworkInterfaces = [.. allNetworkInterfaces.Where(IsActiveInterface)];

        foreach ((NetworkInterface currentInterface, int interfaceIndex) in activeNetworkInterfaces.Select((networkInterface, index) => (networkInterface, index + 1)))
        {
            Console.WriteLine($"  [{interfaceIndex}] {currentInterface.Name}");
            Console.WriteLine($"      Type: {currentInterface.NetworkInterfaceType}");
            Console.WriteLine($"      Status: {currentInterface.OperationalStatus}");

            if (GetInterfaceAddress(currentInterface, AddressFamily.InterNetwork) is { } ipv4Address)
            {
                Console.WriteLine($"      IPv4: {ipv4Address}");
            }

            if (GetInterfaceAddress(currentInterface, AddressFamily.InterNetworkV6) is { } ipv6Address)
            {
                Console.WriteLine($"      IPv6: {ipv6Address}");
            }

            Console.WriteLine();
        }

        return activeNetworkInterfaces switch
        {
            [] => null,
            [var singleInterface] => singleInterface,
            _ => SelectFromMultipleInterfaces(activeNetworkInterfaces)
        };
    }
    catch (Exception exception)
    {
        Console.WriteLine($"[Error] Error detecting network interfaces: {exception.Message}");
        return null;
    }

    static bool IsActiveInterface(NetworkInterface networkInterface)
    {
        try
        {
            if (networkInterface.OperationalStatus is not OperationalStatus.Up)
            {
                return false;
            }

            IPInterfaceProperties interfaceProperties = networkInterface.GetIPProperties();
            return interfaceProperties.UnicastAddresses.Any(addressInfo => addressInfo.Address.AddressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6);
        }
        catch (Exception exception)
        {
            Console.WriteLine($"[Debug] Failed to check interface {networkInterface.Name}: {exception.Message}");
            return false;
        }
    }

    static NetworkInterface? SelectFromMultipleInterfaces(List<NetworkInterface> activeNetworkInterfaces)
    {
        Console.Write($"Select network interface [1-{activeNetworkInterfaces.Count}] or press Enter for auto-selection: ");
        string? userInput = Console.ReadLine();

        if (string.IsNullOrWhiteSpace(userInput))
        {
            return AutoSelectInterface(activeNetworkInterfaces);
        }

        if (int.TryParse(userInput, out int userSelection) && userSelection is > 0 && userSelection <= activeNetworkInterfaces.Count)
        {
            Console.WriteLine($"[Info] Selected interface: {activeNetworkInterfaces[userSelection - 1].Name}");
            return activeNetworkInterfaces[userSelection - 1];
        }

        Console.WriteLine("[Warning] Invalid selection, using first active interface");
        return activeNetworkInterfaces[0];
    }

    static NetworkInterface AutoSelectInterface(List<NetworkInterface> activeNetworkInterfaces)
    {
        return activeNetworkInterfaces.Find(networkInterface => networkInterface.NetworkInterfaceType is NetworkInterfaceType.Ethernet)
            ?? activeNetworkInterfaces.Find(networkInterface => networkInterface.NetworkInterfaceType is NetworkInterfaceType.Wireless80211)
            ?? activeNetworkInterfaces[0];
    }
}

static IPAddress? GetInterfaceAddress(NetworkInterface networkInterface, AddressFamily addressFamily)
{
    try
    {
        return networkInterface.GetIPProperties().UnicastAddresses
            .FirstOrDefault(addressInfo => addressInfo.Address.AddressFamily == addressFamily)
            ?.Address;
    }
    catch
    {
        return null;
    }
}

static void BroadcastMessage(string message, bool isHub, string? hubPeerId, ConcurrentDictionary<string, PeerConnection> peers)
{
    byte[] messageData = Encoding.UTF8.GetBytes(message);

    if (isHub)
    {
        foreach (PeerConnection peerConnection in peers.Values.Where(peer => peer.DataChannel?.readyState is RTCDataChannelState.open))
        {
            peerConnection.DataChannel!.send(messageData);
        }
    }
    else if (peers.TryGetValue(hubPeerId!, out PeerConnection? hubConnection) && hubConnection.DataChannel?.readyState is RTCDataChannelState.open)
    {
        hubConnection.DataChannel.send(messageData);
    }
}

static void SetupDataChannel(RTCDataChannel dataChannel, string peerId, string peerUsername, bool isHub, ConcurrentDictionary<string, PeerConnection> peers)
{
    if (peers.TryGetValue(peerId, out PeerConnection? peerConnection))
    {
        peerConnection.DataChannel = dataChannel;
    }

    dataChannel.onopen += () =>
    {
        string joinMessage = $"[Connected] {peerUsername} joined";
        Console.WriteLine(joinMessage);

        if (isHub)
        {
            byte[] joinMessageData = Encoding.UTF8.GetBytes(joinMessage);
            foreach ((string currentPeerId, PeerConnection currentPeer) in peers.Where(peerEntry => peerEntry.Key != peerId && peerEntry.Value.DataChannel?.readyState is RTCDataChannelState.open))
            {
                currentPeer.DataChannel!.send(joinMessageData);
            }
        }
    };

    dataChannel.onmessage += (_, _, messageData) =>
    {
        string message = Encoding.UTF8.GetString(messageData);
        Console.WriteLine(message);

        if (isHub)
        {
            foreach ((string currentPeerId, PeerConnection currentPeer) in peers.Where(peerEntry => peerEntry.Key != peerId && peerEntry.Value.DataChannel?.readyState is RTCDataChannelState.open))
            {
                currentPeer.DataChannel!.send(messageData);
            }
        }
    };

    dataChannel.onclose += () =>
    {
        string leaveMessage = $"[Disconnected] {peerUsername} left";
        Console.WriteLine(leaveMessage);
        _ = peers.TryRemove(peerId, out _);

        if (isHub)
        {
            byte[] leaveMessageData = Encoding.UTF8.GetBytes(leaveMessage);
            foreach (PeerConnection peerConnection in peers.Values.Where(peer => peer.DataChannel?.readyState is RTCDataChannelState.open))
            {
                peerConnection.DataChannel!.send(leaveMessageData);
            }
        }
    };
}

static async Task RunAsHub(UdpClient? udpClientIpv4, UdpClient? udpClientIpv6, IPEndPoint? multicastEndpointIpv4, IPEndPoint? multicastEndpointIpv6, string username, string myPeerId, bool isRunning, ConcurrentDictionary<string, PeerConnection> peers)
{
    Task hubAnnouncementTask = Task.Run(async () =>
    {
        while (isRunning)
        {
            try
            {
                HubAnnouncement hubAnnouncement = new(Type: "hub", HubId: myPeerId, HubUsername: username);
                byte[] announcementData = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(hubAnnouncement));

                await Task.WhenAll(
                    SendIfValid(udpClientIpv4, multicastEndpointIpv4, announcementData),
                    SendIfValid(udpClientIpv6, multicastEndpointIpv6, announcementData)
                );
            }
            catch (Exception exception)
            {
                Console.WriteLine($"[Warning] Failed to broadcast hub announcement: {exception.Message}");
            }

            await Task.Delay(500);
        }

        static async Task SendIfValid(UdpClient? udpClient, IPEndPoint? multicastEndpoint, byte[] data)
        {
            if (udpClient is not null && multicastEndpoint is not null)
            {
                _ = await udpClient.SendAsync(data, data.Length, multicastEndpoint);
            }
        }
    });

    while (isRunning)
    {
        try
        {
            if (Console.KeyAvailable)
            {
                Console.Write($"[{username}] ");
                string? userMessage = Console.ReadLine();
                if (!string.IsNullOrEmpty(userMessage))
                {
                    if (userMessage.Equals("exit", StringComparison.OrdinalIgnoreCase))
                    {
                        isRunning = false;
                        break;
                    }

                    BroadcastMessage($"[{username}] {userMessage}", true, null, peers);
                }
            }

            await ProcessUdpMessages(udpClientIpv4, udpClientIpv6, multicastEndpointIpv4, multicastEndpointIpv6, myPeerId, peers);
            await Task.Delay(50);
        }
        catch (Exception exception)
        {
            Console.WriteLine($"[Error] Hub loop error: {exception.Message}");
        }
    }

    udpClientIpv4?.Dispose();
    udpClientIpv6?.Dispose();

    static async Task ProcessUdpMessages(UdpClient? udpClientIpv4, UdpClient? udpClientIpv6, IPEndPoint? multicastEndpointIpv4, IPEndPoint? multicastEndpointIpv6, string myPeerId, ConcurrentDictionary<string, PeerConnection> peers)
    {
        await Task.WhenAll(
            ProcessIfAvailable(udpClientIpv4),
            ProcessIfAvailable(udpClientIpv6)
        );

        async Task ProcessIfAvailable(UdpClient? udpClient)
        {
            if (udpClient?.Available > 0)
            {
                await ProcessMessage(udpClient, multicastEndpointIpv4, multicastEndpointIpv6, myPeerId, peers);
            }
        }

        async Task ProcessMessage(UdpClient udpClient, IPEndPoint? multicastEndpointIpv4, IPEndPoint? multicastEndpointIpv6, string myPeerId, ConcurrentDictionary<string, PeerConnection> peers)
        {
            try
            {
                UdpReceiveResult receiveResult = await udpClient.ReceiveAsync();
                string receivedMessage = Encoding.UTF8.GetString(receiveResult.Buffer);

                try
                {
                    if (JsonSerializer.Deserialize<JoinRequest>(receivedMessage) is { Type: "join" } joinRequest
                        && joinRequest.ClientId != myPeerId
                        && !peers.ContainsKey(joinRequest.ClientId))
                    {
                        Console.WriteLine($"[Discovery] Join request from {joinRequest.ClientUsername} ({joinRequest.ClientId})");
                        await AcceptClient(joinRequest.ClientId, joinRequest.ClientUsername, udpClientIpv4, udpClientIpv6, multicastEndpointIpv4, multicastEndpointIpv6, myPeerId, true, peers);
                        return;
                    }

                    if (JsonSerializer.Deserialize<SdpMessage>(receivedMessage) is { Type: "answer" } sdpMessage
                        && sdpMessage.TargetId == myPeerId
                        && sdpMessage.SourceId is not null
                        && peers.TryGetValue(sdpMessage.SourceId, out PeerConnection? peerConnection))
                    {
                        _ = peerConnection.Connection?.setRemoteDescription(new RTCSessionDescriptionInit { type = RTCSdpType.answer, sdp = sdpMessage.Sdp });
                    }
                }
                catch (JsonException)
                {
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine($"[Warning] Error processing message: {exception.Message}");
            }
        }
    }
}

static async Task AcceptClient(string clientId, string clientUsername, UdpClient? udpClientIpv4, UdpClient? udpClientIpv6, IPEndPoint? multicastEndpointIpv4, IPEndPoint? multicastEndpointIpv6, string myPeerId, bool isHub, ConcurrentDictionary<string, PeerConnection> peers)
{
    try
    {
        RTCPeerConnection peerConnection = new();
        PeerConnection newPeerConnection = new(Connection: peerConnection, DataChannel: null, PeerId: clientId, Username: clientUsername);

        if (!peers.TryAdd(clientId, newPeerConnection))
        {
            return;
        }

        RTCDataChannel dataChannel = await peerConnection.createDataChannel("chat", null);
        SetupDataChannel(dataChannel, clientId, clientUsername, isHub, peers);

        RTCSessionDescriptionInit offerDescription = peerConnection.createOffer();
        await peerConnection.setLocalDescription(offerDescription);
        await WaitForIceGathering(peerConnection);

        SdpMessage sdpOfferMessage = new(Type: "offer", Sdp: peerConnection.localDescription.sdp.ToString(), SourceId: myPeerId, TargetId: clientId);
        byte[] offerMessageBytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(sdpOfferMessage));

        for (int retryAttempt = 0; retryAttempt < 5; retryAttempt++)
        {
            await Task.WhenAll(
                SendIfValid(udpClientIpv4, multicastEndpointIpv4, offerMessageBytes),
                SendIfValid(udpClientIpv6, multicastEndpointIpv6, offerMessageBytes)
            );

            await Task.Delay(100);
        }

        static async Task SendIfValid(UdpClient? udpClient, IPEndPoint? multicastEndpoint, byte[] data)
        {
            if (udpClient is not null && multicastEndpoint is not null)
            {
                _ = await udpClient.SendAsync(data, data.Length, multicastEndpoint);
            }
        }
    }
    catch (Exception exception)
    {
        Console.WriteLine($"[Error] Failed to accept client: {exception.Message}");
    }
}

static async Task RunAsClient(UdpClient? udpClientIpv4, UdpClient? udpClientIpv6, IPEndPoint? multicastEndpointIpv4, IPEndPoint? multicastEndpointIpv6, string username, string myPeerId, bool isRunning, string? hubPeerId, ConcurrentDictionary<string, PeerConnection> peers)
{
    DateTime lastJoinRequestTime = DateTime.UtcNow;
    bool connectedToHub = false;

    JoinRequest joinRequest = new(Type: "join", ClientId: myPeerId, ClientUsername: username);
    byte[] joinRequestData = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(joinRequest));

    Console.WriteLine("[Discovery] Sending initial join request...");

    await SendToEndpoints(udpClientIpv4, udpClientIpv6, multicastEndpointIpv4, multicastEndpointIpv6, joinRequestData);

    while (isRunning)
    {
        try
        {
            if (!connectedToHub && (DateTime.UtcNow - lastJoinRequestTime).TotalSeconds >= 1)
            {
                await SendToEndpoints(udpClientIpv4, udpClientIpv6, multicastEndpointIpv4, multicastEndpointIpv6, joinRequestData);
                lastJoinRequestTime = DateTime.UtcNow;
            }

            if (Console.KeyAvailable)
            {
                Console.Write($"[{username}] ");
                string? userMessage = Console.ReadLine();
                if (!string.IsNullOrEmpty(userMessage))
                {
                    if (userMessage.Equals("exit", StringComparison.OrdinalIgnoreCase))
                    {
                        isRunning = false;
                        break;
                    }

                    BroadcastMessage($"[{username}] {userMessage}", false, hubPeerId, peers);
                }
            }

            connectedToHub = await ProcessClientMessages(udpClientIpv4, udpClientIpv6, multicastEndpointIpv4, multicastEndpointIpv6, myPeerId, hubPeerId, connectedToHub, peers) || connectedToHub;
            await Task.Delay(50);
        }
        catch (Exception exception)
        {
            Console.WriteLine($"[Error] Client loop error: {exception.Message}");
        }
    }

    udpClientIpv4?.Dispose();
    udpClientIpv6?.Dispose();

    static async Task SendToEndpoints(UdpClient? udpClientIpv4, UdpClient? udpClientIpv6, IPEndPoint? multicastEndpointIpv4, IPEndPoint? multicastEndpointIpv6, byte[] data)
    {
        await Task.WhenAll(
            SendIfValid(udpClientIpv4, multicastEndpointIpv4, data),
            SendIfValid(udpClientIpv6, multicastEndpointIpv6, data)
        );

        static async Task SendIfValid(UdpClient? udpClient, IPEndPoint? multicastEndpoint, byte[] data)
        {
            if (udpClient is not null && multicastEndpoint is not null)
            {
                _ = await udpClient.SendAsync(data, data.Length, multicastEndpoint);
            }
        }
    }

    static async Task<bool> ProcessClientMessages(UdpClient? udpClientIpv4, UdpClient? udpClientIpv6, IPEndPoint? multicastEndpointIpv4, IPEndPoint? multicastEndpointIpv6, string myPeerId, string? hubPeerId, bool connectedToHub, ConcurrentDictionary<string, PeerConnection> peers)
    {
        return (udpClientIpv4?.Available > 0 && await TryProcessOffer(udpClientIpv4, udpClientIpv6, multicastEndpointIpv4, multicastEndpointIpv6, myPeerId, hubPeerId, connectedToHub, peers))
            || (udpClientIpv6?.Available > 0 && await TryProcessOffer(udpClientIpv6, udpClientIpv4, multicastEndpointIpv6, multicastEndpointIpv4, myPeerId, hubPeerId, connectedToHub, peers));
    }

    static async Task<bool> TryProcessOffer(UdpClient udpClient, UdpClient? alternateUdpClient, IPEndPoint? multicastEndpoint, IPEndPoint? alternateMulticastEndpoint, string myPeerId, string? hubPeerId, bool connectedToHub, ConcurrentDictionary<string, PeerConnection> peers)
    {
        try
        {
            UdpReceiveResult receiveResult = await udpClient.ReceiveAsync();
            string receivedMessage = Encoding.UTF8.GetString(receiveResult.Buffer);

            try
            {
                if (JsonSerializer.Deserialize<SdpMessage>(receivedMessage) is { Type: "offer" } sdpOfferMessage
                    && sdpOfferMessage.TargetId == myPeerId
                    && sdpOfferMessage.SourceId == hubPeerId
                    && !connectedToHub)
                {
                    Console.WriteLine("[Discovery] Received offer from hub, establishing WebRTC connection...");
                    await ConnectToHub(sdpOfferMessage.Sdp, udpClient, alternateUdpClient, multicastEndpoint, alternateMulticastEndpoint, myPeerId, hubPeerId, peers);
                    return true;
                }
            }
            catch (JsonException)
            {
            }
        }
        catch (Exception exception)
        {
            Console.WriteLine($"[Warning] Error receiving message: {exception.Message}");
        }

        return false;
    }
}

static async Task ConnectToHub(string hubOfferSdp, UdpClient? udpClientIpv4, UdpClient? udpClientIpv6, IPEndPoint? multicastEndpointIpv4, IPEndPoint? multicastEndpointIpv6, string myPeerId, string? hubPeerId, ConcurrentDictionary<string, PeerConnection> peers)
{
    try
    {
        RTCPeerConnection peerConnection = new();
        PeerConnection hubPeerConnection = new(Connection: peerConnection, DataChannel: null, PeerId: hubPeerId!, Username: "Hub");

        if (!peers.TryAdd(hubPeerId!, hubPeerConnection))
        {
            return;
        }

        peerConnection.ondatachannel += incomingDataChannel => SetupDataChannel(incomingDataChannel, hubPeerId!, "Hub", false, peers);
        _ = peerConnection.setRemoteDescription(new RTCSessionDescriptionInit { type = RTCSdpType.offer, sdp = hubOfferSdp });

        RTCSessionDescriptionInit answerDescription = peerConnection.createAnswer();
        await peerConnection.setLocalDescription(answerDescription);
        await WaitForIceGathering(peerConnection);

        SdpMessage sdpAnswerMessage = new(Type: "answer", Sdp: peerConnection.localDescription.sdp.ToString(), SourceId: myPeerId, TargetId: hubPeerId!);
        byte[] answerMessageBytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(sdpAnswerMessage));

        for (int retryAttempt = 0; retryAttempt < 5; retryAttempt++)
        {
            await Task.WhenAll(
                SendIfValid(udpClientIpv4, multicastEndpointIpv4, answerMessageBytes),
                SendIfValid(udpClientIpv6, multicastEndpointIpv6, answerMessageBytes)
            );

            await Task.Delay(100);
        }

        Console.WriteLine("[Connected] WebRTC connection established with hub");

        static async Task SendIfValid(UdpClient? udpClient, IPEndPoint? multicastEndpoint, byte[] data)
        {
            if (udpClient is not null && multicastEndpoint is not null)
            {
                _ = await udpClient.SendAsync(data, data.Length, multicastEndpoint);
            }
        }
    }
    catch (Exception exception)
    {
        Console.WriteLine($"[Error] Failed to connect to hub: {exception.Message}");
    }
}

static async Task WaitForIceGathering(RTCPeerConnection peerConnection)
{
    TaskCompletionSource<bool> iceGatheringCompletionSource = new();
    peerConnection.onicegatheringstatechange += iceGatheringState =>
    {
        if (iceGatheringState is RTCIceGatheringState.complete)
        {
            _ = iceGatheringCompletionSource.TrySetResult(true);
        }
    };

    _ = await Task.WhenAny(iceGatheringCompletionSource.Task, Task.Delay(5000));
}

file sealed record PeerConnection(RTCPeerConnection? Connection, RTCDataChannel? DataChannel, string PeerId, string Username)
{
    public RTCDataChannel? DataChannel { get; set; } = DataChannel;
}

file sealed record HubAnnouncement(string Type, string HubId, string HubUsername);

file sealed record JoinRequest(string Type, string ClientId, string ClientUsername);

file sealed record SdpMessage(string Type, string Sdp, string SourceId, string TargetId);
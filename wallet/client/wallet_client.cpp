// Copyright 2018 The Beam Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "wallet_client_internal.h"

namespace
{
using namespace beam;
using namespace beam::wallet;

constexpr size_t kCollectorBufferSize = 100;

using WalletSubscriber = ScopedSubscriber<wallet::IWalletObserver, wallet::Wallet>;

struct WalletModelBridge : public Bridge<IWalletModelAsync>
{
    BRIDGE_INIT(WalletModelBridge);

    void sendMoney(const wallet::WalletID& receiverID, const std::string& comment, Amount amount, Amount fee) override
    {
        typedef void(IWalletModelAsync::*SendMoneyType)(const wallet::WalletID&, const std::string&, Amount, Amount);
        call_async((SendMoneyType)&IWalletModelAsync::sendMoney, receiverID, comment, amount, fee);
    }

    void sendMoney(const wallet::WalletID& senderID, const wallet::WalletID& receiverID, const std::string& comment, Amount amount, Amount fee) override
    {
        typedef void(IWalletModelAsync::*SendMoneyType)(const wallet::WalletID &, const wallet::WalletID &, const std::string &, Amount, Amount);
        call_async((SendMoneyType)&IWalletModelAsync::sendMoney, senderID, receiverID, comment, amount, fee);
    }

    void startTransaction(TxParameters&& parameters) override
    {
        call_async(&IWalletModelAsync::startTransaction, std::move(parameters));
    }

    void syncWithNode() override
    {
        call_async(&IWalletModelAsync::syncWithNode);
    }

    void calcChange(Amount amount, Amount fee, Asset::ID assetId) override
    {
        call_async(&IWalletModelAsync::calcChange, amount, fee, assetId);
    }

    void selectCoins(Amount amount, Amount beforehandMinFee, Asset::ID assetId, bool isShielded /* = false */) override
    {
        typedef void(IWalletModelAsync::* MethodType)(Amount, Amount, Asset::ID, bool);
        call_async((MethodType)&IWalletModelAsync::selectCoins, amount, beforehandMinFee, assetId, isShielded);
    }

    void selectCoins(Amount amount, Amount beforehandMinFee, Asset::ID assetId, bool isShielded, AsyncCallback<const CoinsSelectionInfo&>&& callback) override
    {
        typedef void(IWalletModelAsync::* MethodType)(Amount, Amount, Asset::ID, bool, AsyncCallback<const CoinsSelectionInfo&>&&);
        call_async((MethodType)&IWalletModelAsync::selectCoins, amount, beforehandMinFee, assetId, isShielded, callback);
    }

    void getWalletStatus() override
    {
        call_async(&IWalletModelAsync::getWalletStatus);
    }

    void getTransactions() override
    {
        typedef void(IWalletModelAsync::* MethodType)();
        call_async((MethodType)&IWalletModelAsync::getTransactions);
    }

    void getTransactions(AsyncCallback<const std::vector<TxDescription>&>&& callback) override
    {
        typedef void(IWalletModelAsync::* MethodType)(AsyncCallback<const std::vector<TxDescription>&>&&);
        call_async((MethodType)&IWalletModelAsync::getTransactions, callback);
    }

    void getTransactionsSmoothly() override
    {
        typedef void(IWalletModelAsync::* MethodType)();
        call_async((MethodType)&IWalletModelAsync::getTransactionsSmoothly);
    }

    void getAllUtxosStatus() override
    {
        call_async(&IWalletModelAsync::getAllUtxosStatus);
    }

    void getAddresses(bool own) override
    {
        typedef void(IWalletModelAsync::* MethodType)(bool);
        call_async((MethodType)&IWalletModelAsync::getAddresses, own);
    }

#ifdef BEAM_ASSET_SWAP_SUPPORT
    void getDexOrders() override
    {
        call_async(&IWalletModelAsync::getDexOrders);
    }

    void getDexOrder(const DexOrderID& orderId) override
    {
        call_async(&IWalletModelAsync::getDexOrder, orderId);
    }

    void publishDexOrder(const DexOrder& order) override
    {
        call_async(&IWalletModelAsync::publishDexOrder, order);
    }

    void cancelDexOrder(const DexOrderID& orderId) override
    {
        call_async(&IWalletModelAsync::cancelDexOrder, orderId);
    }
#endif  // BEAM_ASSET_SWAP_SUPPORT

    void loadFullAssetsList() override
    {
        call_async(&IWalletModelAsync::loadFullAssetsList);
    }


#ifdef BEAM_ASSET_SWAP_SUPPORT
    void loadDexOrderParams() override
    {
        call_async(&IWalletModelAsync::loadDexOrderParams);
    }

    void storeDexOrderParams(const beam::ByteBuffer& params) override
    {
        call_async(&IWalletModelAsync::storeDexOrderParams, params);
    }
#endif  // BEAM_ASSET_SWAP_SUPPORT

    void cancelTx(const wallet::TxID& id) override
    {
        call_async(&IWalletModelAsync::cancelTx, id);
    }

    void deleteTx(const wallet::TxID& id) override
    {
        call_async(&IWalletModelAsync::deleteTx, id);
    }

    void getCoinsByTx(const wallet::TxID& id) override
    {
        call_async(&IWalletModelAsync::getCoinsByTx, id);
    }

    void saveAddress(const wallet::WalletAddress& address) override
    {
        call_async(&IWalletModelAsync::saveAddress, address);
    }

    void generateNewAddress() override
    {
        typedef void(IWalletModelAsync::* MethodType)();
        call_async((MethodType)&IWalletModelAsync::generateNewAddress);
    }

    void generateNewAddress(AsyncCallback<const WalletAddress&>&& callback) override
    {
        typedef void(IWalletModelAsync::* MethodType)(AsyncCallback<const WalletAddress&>&&);
        call_async((MethodType)&IWalletModelAsync::generateNewAddress, std::move(callback));
    }

    void generateToken(wallet::TokenType type, Amount amount, Asset::ID aid, std::string sVer, bool newAddress, AsyncCallback<std::string&&>&& callback) override
    {
        typedef void(IWalletModelAsync::* MethodType)(wallet::TokenType, Amount, Asset::ID, std::string, bool, AsyncCallback<std::string&&>&&);
        call_async((MethodType)&IWalletModelAsync::generateToken, type, amount, aid, std::move(sVer), newAddress, std::move(callback));
    }

    void deleteAddress(const WalletID& addr) override
    {
        call_async(&IWalletModelAsync::deleteAddress, addr);
    }

    void updateAddress(const WalletID& address, const std::string& name, beam::Timestamp expirationTime) override
    {
        typedef void(IWalletModelAsync::* MethodType)(const WalletID&, const std::string&, beam::Timestamp);
        call_async((MethodType)&IWalletModelAsync::updateAddress, address, name, expirationTime);
    }

     void updateAddress(const WalletID& address, const std::string& name, WalletAddress::ExpirationStatus expirationStatus) override
     {
        typedef void(IWalletModelAsync::* MethodType)(const WalletID&, const std::string&, WalletAddress::ExpirationStatus);
        call_async((MethodType)&IWalletModelAsync::updateAddress, address, name, expirationStatus);
     }

    void activateAddress(const WalletID& address) override
    {
        call_async(&IWalletModelAsync::activateAddress, address);
    }

    void getAddress(const WalletID& id) override
    {
        typedef void(IWalletModelAsync::* MethodType)(const WalletID&);
        call_async((MethodType)&IWalletModelAsync::getAddress, id);
    }

    void getAddress(const WalletID& addr, AsyncCallback <const boost::optional<WalletAddress>&, size_t>&& callback) override
    {
        typedef void(IWalletModelAsync::* MethodType)(const WalletID&, AsyncCallback<const boost::optional<WalletAddress>&, size_t>&&);
        call_async((MethodType)&IWalletModelAsync::getAddress, addr, std::move(callback));
    }

    void getAddressByToken(const std::string& token, AsyncCallback <const boost::optional<WalletAddress>&, size_t>&& callback) override
    {
        call_async(&IWalletModelAsync::getAddressByToken, token, std::move(callback));
    }

    void deleteAddressByToken(const std::string& token) override
    {
        call_async(&IWalletModelAsync::deleteAddressByToken, token);
    }

    void verifyOnHw(const std::string& token) override
    {
        call_async(&IWalletModelAsync::verifyOnHw, token);
    }

    void saveVouchers(const ShieldedVoucherList& v, const WalletID& walletID) override
    {
        call_async(&IWalletModelAsync::saveVouchers, v, walletID);
    }

    void setNodeAddress(const std::string& addr) override
    {
        call_async(&IWalletModelAsync::setNodeAddress, addr);
    }

    void changeWalletPassword(const SecString& pass) override
    {
        // TODO: should be investigated, don't know how to "move" SecString into lambda
        std::string passStr(pass.data(), pass.size());

        call_async(&IWalletModelAsync::changeWalletPassword, passStr);
    }

    void getNetworkStatus() override
    {
        call_async(&IWalletModelAsync::getNetworkStatus);
    }

    void rescan() override
    {
        call_async(&IWalletModelAsync::rescan);
    }

    void exportPaymentProof(const wallet::TxID& id) override
    {
        call_async(&IWalletModelAsync::exportPaymentProof, id);
    }

    void importSlatepack(const std::string& text) override
    {
        call_async(&IWalletModelAsync::importSlatepack, text);
    }

    void commitSlatepack(const std::string& txId) override
    {
        call_async(&IWalletModelAsync::commitSlatepack, txId);
    }

    void cancelSlatepack(const std::string& txId) override
    {
        call_async(&IWalletModelAsync::cancelSlatepack, txId);
    }

    void checkNetworkAddress(const std::string& addr) override
    {
        call_async(&IWalletModelAsync::checkNetworkAddress, addr);
    }

    void importRecovery(const std::string& path) override
    {
        call_async(&IWalletModelAsync::importRecovery, path);
    }

    void importDataFromJson(const std::string& data) override
    {
        call_async(&IWalletModelAsync::importDataFromJson, data);
    }

    void exportDataToJson() override
    {
        call_async(&IWalletModelAsync::exportDataToJson);
    }

    void exportTxHistoryToCsv() override
    {
        call_async(&IWalletModelAsync::exportTxHistoryToCsv);
    }

    void switchOnOffExchangeRates(bool isActive) override
    {
        call_async(&IWalletModelAsync::switchOnOffExchangeRates, isActive);
    }

    void switchOnOffNotifications(Notification::Type type, bool isActive) override
    {
        call_async(&IWalletModelAsync::switchOnOffNotifications, type, isActive);
    }
        
    void getNotifications() override
    {
        call_async(&IWalletModelAsync::getNotifications);
    }

    void markNotificationAsRead(const ECC::uintBig& id) override
    {
        call_async(&IWalletModelAsync::markNotificationAsRead, id);
    }

    void deleteNotification(const ECC::uintBig& id) override
    {
        call_async(&IWalletModelAsync::deleteNotification, id);
    }

    void getExchangeRates() override
    {
        call_async(&IWalletModelAsync::getExchangeRates);
    }

    void getVerificationInfo() override
    {
        call_async(&IWalletModelAsync::getVerificationInfo);
    }

    void getPublicAddress() override
    {
        call_async(&IWalletModelAsync::getPublicAddress);
    }

    void getAssetInfo(Asset::ID assetId) override
    {
        call_async(&IWalletModelAsync::getAssetInfo, assetId);
    }

    void makeIWTCall(std::function<boost::any()>&& function, AsyncCallback<const boost::any&>&& resultCallback) override
    {
        call_async(&IWalletModelAsync::makeIWTCall, std::move(function), std::move(resultCallback));
    }

    void callShader(std::vector<uint8_t>&& shader, std::string&& args, CallShaderCallback&& cback) override
    {
        typedef void(IWalletModelAsync::* MethodType)(std::vector<uint8_t>&&, std::string&&, CallShaderCallback&&);
        call_async((MethodType)&IWalletModelAsync::callShader, std::move(shader), std::move(args), std::move(cback));
    }

    void callShader(std::string&& shaderFile, std::string&& args, CallShaderCallback&& cback) override
    {
        typedef void(IWalletModelAsync::* MethodType)(std::string&&, std::string&&, CallShaderCallback&&);
        call_async((MethodType)&IWalletModelAsync::callShader, std::move(shaderFile), std::move(args), std::move(cback));
    }

    void callShaderAndStartTx(beam::ByteBuffer&& shader, std::string&& args, CallShaderAndStartTxCallback&& cback) override
    {
        typedef void(IWalletModelAsync::* MethodType)(std::vector<uint8_t>&&, std::string&&, CallShaderAndStartTxCallback&&);
        call_async((MethodType)&IWalletModelAsync::callShaderAndStartTx, std::move(shader), std::move(args), std::move(cback));
    }

    void callShaderAndStartTx(std::string&& shaderFile, std::string&& args, CallShaderAndStartTxCallback&& cback) override
    {
        typedef void(IWalletModelAsync::* MethodType)(std::string&&, std::string&&, CallShaderAndStartTxCallback&&);
        call_async((MethodType)&IWalletModelAsync::callShaderAndStartTx, std::move(shaderFile), std::move(args), std::move(cback));
    }

    void processShaderTxData(beam::ByteBuffer&& data, ProcessShaderTxDataCallback&& cback) override
    {
        call_async(&IWalletModelAsync::processShaderTxData, std::move(data), std::move(cback));
    }

    void setMaxPrivacyLockTimeLimitHours(uint8_t limit) override
    {
        call_async(&IWalletModelAsync::setMaxPrivacyLockTimeLimitHours, limit);
    }

    void getMaxPrivacyLockTimeLimitHours(AsyncCallback<uint8_t>&& callback) override
    {
        call_async(&IWalletModelAsync::getMaxPrivacyLockTimeLimitHours, std::move(callback));
    }

    void setCoinConfirmationsOffset(uint32_t limit) override
    {
        call_async(&IWalletModelAsync::setCoinConfirmationsOffset, limit);
    }

    void getCoinConfirmationsOffset(AsyncCallback<uint32_t>&& callback) override
    {
        call_async(&IWalletModelAsync::getCoinConfirmationsOffset, std::move(callback));
    }

    void removeRawSeedPhrase() override
    {
        call_async(&IWalletModelAsync::removeRawSeedPhrase);
    }

    void readRawSeedPhrase(AsyncCallback<const std::string&>&& callback) override
    {
        call_async(&IWalletModelAsync::readRawSeedPhrase, std::move(callback));
    }

    void getAppsList(AppsListCallback&& callback) override
    {
        call_async(&IWalletModelAsync::getAppsList, std::move(callback));
    }

    void markAppNotificationAsRead(const TxID& id) override
    {
        call_async(&IWalletModelAsync::markAppNotificationAsRead, id);
    }

    void enableBodyRequests(bool value) override
    {
        call_async(&IWalletModelAsync::enableBodyRequests, value);
    }

    void sendInstantMessage(const WalletID& peerID, const WalletID& myID, ByteBuffer&& message) override
    {
        call_async(&IWalletModelAsync::sendInstantMessage, peerID, myID, std::move(message));
    }

    void getChats() override
    {
        call_async(&IWalletModelAsync::getChats);
    }

    void markIMsasRead(const std::vector<std::pair<Timestamp, WalletID>>&& ims) override
    {
        call_async(&IWalletModelAsync::markIMsasRead, std::move(ims));
    }

    void getInstantMessages(const WalletID& peerID) override
    {
        call_async(&IWalletModelAsync::getInstantMessages, peerID);
    }

    void removeChat(const WalletID& peerID) override
    {
        call_async(&IWalletModelAsync::removeChat, peerID);
    }
};
}

namespace beam::wallet
{
    WalletClient::WalletClient(const Rules& rules, IWalletDB::Ptr walletDB, OpenDBFunction&& walletDBFunc, const std::string& nodeAddr, io::Reactor::Ptr reactor)
        : m_rules(rules)
        , m_walletDB(walletDB)
        , m_reactor{ reactor ? reactor : io::Reactor::create() }
        , m_async{ make_shared<WalletModelBridge>(*(static_cast<IWalletModelAsync*>(this)), *m_reactor) }
        , m_connectedNodesCount(0)
        , m_trustedConnectionCount(0)
        , m_initialNodeAddrStr(nodeAddr)
        , m_CoinChangesCollector(kCollectorBufferSize, m_reactor, [this](auto action, const auto& items) { onNormalCoinsChanged(action, items); })
        , m_ShieldedCoinChangesCollector(kCollectorBufferSize, m_reactor, [this](auto action, const auto& items) { onShieldedCoinChanged(action, items); })
        , m_AddressChangesCollector(kCollectorBufferSize, m_reactor, [this](auto action, const auto& items) { onAddressesChanged(action, items); })
        , m_TransactionChangesCollector(kCollectorBufferSize, m_reactor, [this](auto action, const auto& items) {
             onTxStatus(action, items);
              })
        , m_shieldedPer24hFilter(std::make_unique<Filter>(kShieldedPer24hFilterSize))
        , m_openDBFunc(std::move(walletDBFunc))
    {
        m_ainfoDelayed = io::Timer::create(*m_reactor);
        m_balanceDelayed = io::Timer::create(*m_reactor);
    }

    WalletClient::WalletClient(const Rules& rules, IWalletDB::Ptr walletDB, const std::string& nodeAddr, io::Reactor::Ptr reactor)
        : WalletClient(rules, walletDB, {}, nodeAddr, reactor)
    {

    }

    WalletClient::WalletClient(const Rules& rules, OpenDBFunction&& walletDBFunc, const std::string& nodeAddr, io::Reactor::Ptr reactor)
        : WalletClient(rules, nullptr, std::move(walletDBFunc), nodeAddr, reactor)
    {
    }

    WalletClient::~WalletClient()
    {
        // reactor thread should be already stopped here, but just in case
        // this call is unsafe and may result in crash if reactor is not stopped
        assert(!m_thread);
        stopReactor();
    }

    void WalletClient::stopReactor(bool detachThread/* = false*/)
    {
        try
        {
            if (isRunning())
            {
                assert(m_reactor);
                m_reactor->stop();
                if (!detachThread)
                {
                    m_thread->join();
                }
                else
                {
                    m_thread->detach();
                }
            }
            m_thread.reset();
        }
        catch (const std::exception& e)
        {
            BEAM_LOG_UNHANDLED_EXCEPTION() << "what = " << e.what();
        }
        catch (...) {
            BEAM_LOG_UNHANDLED_EXCEPTION();
        }
    }

    IWalletDB::Ptr WalletClient::getWalletDB()
    {
        return m_walletDB;
    }

    void WalletClient::postFunctionToClientContext(MessageFunction&& func)
    {
        onPostFunctionToClientContext(std::move(func));
    }

    //
    // UI thread. Methods below should be called from main thread
    //
    Version WalletClient::getLibVersion() const
    {
        // TODO: replace with current wallet library version
        return beam::Version
        {
            0,
            0,
            0
        };
    }

    uint32_t WalletClient::getClientRevision() const
    {
        return 0;
    }

    void WalletClient::start( std::map<Notification::Type,bool> activeNotifications,
                              bool withExchangeRates,
                              std::shared_ptr<std::unordered_map<TxType, BaseTransaction::Creator::Ptr>> txCreators)
    {
        if (isRunning())
        {
            return;
        }
        m_thread = std::make_shared<MyThread>([this, withExchangeRates, txCreators, activeNotifications]()
        {
            try
            {
                Rules::Scope scopeRules(getRules());
                io::Reactor::Scope scope(*m_reactor);
#ifndef __EMSCRIPTEN__
                static const unsigned LOG_ROTATION_PERIOD_SEC = 3 * 3600; // 3 hours
                static const unsigned LOG_CLEANUP_PERIOD_SEC = 120 * 3600; // 5 days
                LogRotation logRotation(*m_reactor, LOG_ROTATION_PERIOD_SEC, LOG_CLEANUP_PERIOD_SEC);
#endif // !__EMSCRIPTEN__

                if (!m_walletDB)
                {
                    if (m_openDBFunc)
                    {
                        m_walletDB = m_openDBFunc();
                    }
                    else
                    {
                        throw std::runtime_error("WalletClient: database is not provided");
                    }
                }

                auto wallet = make_shared<Wallet>(m_walletDB);
                m_wallet = wallet;

                if (txCreators)
                {
                    for (auto&[txType, creator] : *txCreators)
                    {
                        wallet->RegisterTransactionType(txType, creator);
                    }
                }

                struct MyWidgetNotify :public Wallet::IWidgetNotify {
                    WalletClient& m_This;
                    MyWidgetNotify(WalletClient& x) :m_This(x)
                    {
                        auto w = m_This.m_wallet.lock();
                        if (w)
                            w->m_pWidgetNotify = this;
                    }
                    ~MyWidgetNotify()
                    {
                        auto w = m_This.m_wallet.lock();
                        if (w)
                            w->m_pWidgetNotify = nullptr;
                    }

                    void OnWriteStream(const std::string& s, const Blob& b, uint32_t iStream) override
                    {
                        ByteBuffer buf;
                        b.Export(buf);
                        WalletClient& wc = m_This;

                        m_This.postFunctionToClientContext([&wc, buf1 = std::move(buf), sName = s, iStream]()
                        {
                            std::string s2 = std::move(sName);
                            ByteBuffer buf2 = std::move(buf1);
                            wc.onWidgetWrite(std::move(s2), std::move(buf2), iStream);
                        });

                    }

                } wn(*this);

                updateClientState(getStatus());

                std::vector<io::Address> fallbackAddresses;
                storage::getBlobVar(*m_walletDB, FallbackPeers, fallbackAddresses);
                auto nodeNetwork = make_shared<NodeNetwork>(*wallet, m_initialNodeAddrStr, std::move(fallbackAddresses));
                m_nodeNetwork = nodeNetwork;

                using NodeNetworkSubscriber = ScopedSubscriber<INodeConnectionObserver, NodeNetwork>;
                auto nodeNetworkSubscriber =
                    std::make_unique<NodeNetworkSubscriber>(static_cast<INodeConnectionObserver*>(this), nodeNetwork);

                std::shared_ptr<WalletNetworkViaBbs> walletNetwork;
                if (SbbsPolicy::Disabled != m_SbbsPolicy)
                {
                    walletNetwork = make_shared<WalletNetworkViaBbs>(*wallet, nodeNetwork, m_walletDB);
                    m_walletNetwork = walletNetwork;
                }
                wallet->SetNodeEndpoint(nodeNetwork);
                if (walletNetwork)
                {
                    wallet->AddMessageEndpoint(walletNetwork);
                    if (SbbsPolicy::DisabledForTxs == m_SbbsPolicy)
                        wallet->m_pBlockedForTxs = walletNetwork.get();
                }

                auto slatepackEndpoint = make_shared<SlatepackEndpoint>(*wallet, m_walletDB,
                    [this](const TxID& txID, const std::string& armored)
                    {
                        onSlatepackReady(txID, armored);
                    });
                m_slatepackEndpoint = slatepackEndpoint;
                wallet->AddMessageEndpoint(slatepackEndpoint);

                wallet->ResumeAllTransactions();

                updateMaxPrivacyStatsImpl(getStatus());

                auto wallet_subscriber = make_unique<WalletSubscriber>(static_cast<IWalletObserver*>(this), wallet);

                // Notification center initialization
                m_notificationCenter =
                    make_shared<NotificationCenter>(*m_walletDB, activeNotifications, m_reactor->shared_from_this());
                using NotificationsSubscriber = ScopedSubscriber<INotificationsObserver, NotificationCenter>;

                struct MyNotificationsObserver : INotificationsObserver
                {
                    WalletClient& m_client;
                    explicit MyNotificationsObserver(WalletClient& client) : m_client(client) {}
                    void onNotificationsChanged(ChangeAction action, const std::vector<Notification>& items) override
                    {
                        m_client.updateNotifications();
                        static_cast<INotificationsObserver&>(m_client).onNotificationsChanged(action, items);
                    }
                } notificationObserver(*this);

                auto notificationsSubscriber =
                    make_unique<NotificationsSubscriber>(&notificationObserver, m_notificationCenter);
                updateNotifications();

                using WalletDbSubscriber = ScopedSubscriber<IWalletDbObserver, IWalletDB>;
                auto notificationsDbSubscriber = make_unique<WalletDbSubscriber>(static_cast<IWalletDbObserver*>(m_notificationCenter.get()), m_walletDB);

                // Broadcast router and broadcast message consumers. All of them work over SBBS, so with
                // SBBS disabled none is created. They must live until the reactor stops, hence declared here.
                using WalletUpdatesSubscriber = ScopedSubscriber<INewsObserver, WalletUpdatesProvider>;
                using ExchangeRatesSubscriber = ScopedSubscriber<IExchangeRatesObserver, ExchangeRateProvider>;
                using VerificationSubscriber = ScopedSubscriber<IVerificationObserver, VerificationProvider>;

                std::shared_ptr<BroadcastRouter> broadcastRouter;
                std::shared_ptr<BroadcastMsgValidator> broadcastValidator;
                std::shared_ptr<WalletUpdatesProvider> walletUpdatesProvider;
                std::shared_ptr<ExchangeRateProvider> exchangeRateProvider;
                std::unique_ptr<WalletUpdatesSubscriber> walletUpdatesSubscriber;
                std::unique_ptr<ExchangeRatesSubscriber> ratesSubscriber;
                std::shared_ptr<VerificationProvider> verificationProvider;
                std::unique_ptr<VerificationSubscriber> verificationSubscriber;
#ifdef BEAM_ASSET_SWAP_SUPPORT
                using DexBoardSubscriber = ScopedSubscriber<DexBoard::IObserver, DexBoard>;
                using DexWalletSubscriber = ScopedSubscriber<wallet::ISimpleSwapHandler, wallet::Wallet>;
                std::shared_ptr<DexBoard> dexBoard;
                std::unique_ptr<WalletDbSubscriber> dexWDBSubscriber;
                std::unique_ptr<DexBoardSubscriber> dexBoardSubscriber;
                std::unique_ptr<DexWalletSubscriber> dexWalletSubscriber;
#endif

                if (walletNetwork)
                {
                    broadcastRouter = make_shared<BroadcastRouter>(nodeNetwork, *walletNetwork, std::make_shared<BroadcastRouter::BbsTsHolder>(m_walletDB));
                    m_broadcastRouter = broadcastRouter;

                    // Broadcast validator initialization. It verifies messages signatures.
                    broadcastValidator = make_shared<BroadcastMsgValidator>();
                    {
                        PeerID key;
                        if (BroadcastMsgValidator::stringToPublicKey(get_BroadcastValidatorPublicKey(), key))
                        {
                            broadcastValidator->setPublisherKeys( { key } );
                        }
                    }

                    // Other content providers using broadcast messages
                    walletUpdatesProvider = make_shared<WalletUpdatesProvider>(*broadcastRouter, *broadcastValidator);
                    exchangeRateProvider = make_shared<ExchangeRateProvider>(*broadcastRouter, *broadcastValidator, *m_walletDB, withExchangeRates);
                    m_exchangeRateProvider = exchangeRateProvider;
                    m_walletUpdatesProvider = walletUpdatesProvider;
                    walletUpdatesSubscriber = make_unique<WalletUpdatesSubscriber>(static_cast<INewsObserver*>(m_notificationCenter.get()), walletUpdatesProvider);
                    ratesSubscriber = make_unique<ExchangeRatesSubscriber>(static_cast<IExchangeRatesObserver*>(this), exchangeRateProvider);

                    //
                    // Assets verification
                    //
                    verificationProvider = make_shared<VerificationProvider>(*broadcastRouter, *broadcastValidator, *m_walletDB);
                    m_verificationProvider = verificationProvider;
                    verificationSubscriber = make_unique<VerificationSubscriber>(static_cast<IVerificationObserver*>(this), verificationProvider);

                    //
                    // DEX
                    //
#ifdef BEAM_ASSET_SWAP_SUPPORT
                    dexBoard = make_shared<DexBoard>(*broadcastRouter, *wallet, *m_walletDB);
                    dexWDBSubscriber = make_unique<WalletDbSubscriber>(static_cast<IWalletDbObserver*>(dexBoard.get()), m_walletDB);
                    dexBoardSubscriber = make_unique<DexBoardSubscriber>(static_cast<DexBoard::IObserver*>(this), dexBoard);
                    dexWalletSubscriber = make_unique<DexWalletSubscriber>(static_cast<ISimpleSwapHandler*>(dexBoard.get()), wallet);

                    _dex = dexBoard;
#endif  // BEAM_ASSET_SWAP_SUPPORT
                }


                //
                // Shaders
                //
                auto clientShaders = IShadersManager::CreateInstance(*wallet, "", "", 0);
                _clientShaders = clientShaders;

                nodeNetwork->tryToConnect();
                m_reactor->run_ex([&wallet, &nodeNetwork](){
                    wallet->CleanupNetwork();
                    nodeNetwork->Disconnect();
                });

                wallet->CleanupNetwork();
                nodeNetwork->Disconnect();

                assert(clientShaders.use_count() == 1);
                clientShaders.reset();

                assert(walletNetwork.use_count() <= 1);
                walletNetwork.reset();

                nodeNetworkSubscriber.reset();
                broadcastRouter.reset();
                assert(nodeNetwork.use_count() == 1);
                nodeNetwork.reset();

                m_balanceDelayed->cancel();
                m_ainfoDelayed->cancel();
            }
            catch (const runtime_error& ex)
            {
                BEAM_LOG_ERROR() << ex.what();
                FailedToStartWallet();
            }
            catch (const std::exception& ex)
            {
                BEAM_LOG_UNHANDLED_EXCEPTION() << "what = " << ex.what();
            }
            /*catch (...) {
                BEAM_LOG_UNHANDLED_EXCEPTION();
            }*/
            onStopped();
        });
    }

    IWalletModelAsync::Ptr WalletClient::getAsync()
    {
        return m_async;
    }

    Wallet::Ptr WalletClient::getWallet()
    {
        auto sp = m_wallet.lock();
        return sp;
    }

    NodeNetwork::Ptr WalletClient::getNodeNetwork()
    {
        auto sp = m_nodeNetwork.lock();
        return sp;
    }

    std::string WalletClient::getNodeAddress() const
    {
        if (auto s = m_nodeNetwork.lock())
        {
            return s->getNodeAddress();
        }
        else
        {
            return m_initialNodeAddrStr;
        }
    }

    bool WalletClient::isRunning() const
    {
        return m_thread && m_thread->joinable();
    }
}

#include "wallet/wallet_network.h"
#include "core/common.h"

#include "node.h"
#include "wallet/wallet.h"
#include "wallet/keychain.h"
#include "wallet/wallet_network.h"
#include "core/ecc_native.h"
#include "core/serialization_adapters.h"
#include "utility/logger.h"
#include <iomanip>

#include <boost/program_options.hpp>
#include <fstream>
#include <iterator>

namespace po = boost::program_options;
using namespace std;
using namespace beam;
using namespace ECC;

namespace cli
{
    const char* HELP = "help";
    const char* HELP_FULL = "help,h";
    const char* MODE = "mode";
    const char* PORT = "port";
    const char* PORT_FULL = "port,p";
    const char* DEBUG = "debug";
    const char* DEBUG_FULL = "debug,d";
    const char* STORAGE = "storage";
    const char* MINING_THREADS = "mining_threads";
	const char* VERIFICATION_THREADS = "verification_threads";
	const char* MINER_ID = "miner_id";
	const char* NODE_PEER = "peer";
	const char* PASS = "pass";
    const char* AMOUNT = "amount";
    const char* AMOUNT_FULL = "amount,a";
    const char* RECEIVER_ADDR = "receiver_addr";
    const char* RECEIVER_ADDR_FULL = "receiver_addr,r";
    const char* NODE_ADDR = "node_addr";
    const char* NODE_ADDR_FULL = "node_addr,n";
    const char* COMMAND = "command";
    const char* NODE = "node";
    const char* WALLET = "wallet";
    const char* LISTEN = "listen";
	const char* TREASURY = "treasury";
	const char* TREASURY_BLOCK = "treasury_path";
    const char* INIT = "init";
    const char* SEND = "send";
    const char* INFO = "info";
    const char* WALLET_SEED = "wallet_seed";
}
namespace beam
{
    std::ostream& operator<<(std::ostream& os, Coin::Status s)
    {
        os << "[";
        switch (s)
        {
        case Coin::Locked: os << "Locked"; break;
        case Coin::Spent: os << "Spent"; break;
        case Coin::Unconfirmed: os << "Unconfirmed"; break;
        case Coin::Unspent: os << "Unspent"; break;
        default:
            assert(false && "Unknown coin status");
        }
        os << "]";
        return os;
    }

    std::ostream& operator<<(std::ostream& os, KeyType keyType)
    {
        os << "[";
        switch (keyType)
        {
        case KeyType::Coinbase: os << "Coinbase"; break;
        case KeyType::Comission: os << "Commission"; break;
        case KeyType::Kernel: os << "Kernel"; break;
        case KeyType::Regular: os << "Regualar"; break;
        default:
            assert(false && "Unknown key type");
        }
        os << "]";
        return os;
    }
}
namespace
{
    void printHelp(const po::options_description& options)
    {
        cout << options << std::endl;
    }


}

struct SerializerFile {

	SerializerFile() : _oa(m_File) {}

	template <typename T> SerializerFile& operator&(const T& object) {
		_oa & object;
		return *this;
	}

	struct Stream :public std::ofstream
	{
		size_t write(const void* p, size_t n)
		{
			std::ofstream::write((char*)p, n);
			return fail() ? 0 : n;
		}
	} m_File;

private:
	yas::binary_oarchive<Stream, SERIALIZE_OPTIONS> _oa;
};

struct TreasuryBlockGenerator
{
	std::string m_sPath;
	IKeyChain* m_pKeyChain;

	Block::Body m_Block;
	ECC::Scalar::Native m_Offset;

	std::vector<Coin> m_Coins;
	std::vector<std::pair<Height, ECC::Scalar::Native> > m_vIncubationAndKeys;

	std::mutex m_Mutex;
	std::vector<std::thread> m_vThreads;

	int Generate(uint32_t nCount, Height dh);
private:
	void Proceed(uint32_t i);
};

int TreasuryBlockGenerator::Generate(uint32_t nCount, Height dh)
{
	if (m_sPath.empty())
	{
		LOG_ERROR() << "Treasury block path not specified";
		return -1;
	}

	m_Block.ZeroInit();

	{
		std::ifstream f(m_sPath, std::ifstream::binary);
		if (!f.fail())
		{
			std::vector<char> vContents((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
			if (!vContents.empty())
			{
				Deserializer der;
				der.reset(&vContents.at(0), vContents.size());
				der & m_Block;

				LOG_INFO() << "Treasury block is non-empty, appending.";
			}
		}
	}

	m_Offset = m_Block.m_Offset;
	m_Offset = -m_Offset;

	LOG_INFO() << "Generating coins...";

	m_Coins.resize(nCount);
	m_vIncubationAndKeys.resize(nCount);

	Height h = 0;

	for (uint32_t i = 0; i < nCount; i++, h += dh)
	{
		Coin& coin = m_Coins[i];
		coin.m_key_type = KeyType::Regular;
		coin.m_amount = Block::Rules::Coin * 10;
		coin.m_status = Coin::Unconfirmed;
		coin.m_height = h + Block::Rules::HeightGenesis;

		m_vIncubationAndKeys[i].first = h;
		m_vIncubationAndKeys[i].second = m_pKeyChain->calcKey(coin);
	}

	m_pKeyChain->store(m_Coins);

	m_vThreads.resize(std::thread::hardware_concurrency());
	assert(!m_vThreads.empty());

	for (uint32_t i = 0; i < m_vThreads.size(); i++)
		m_vThreads[i] = std::thread(&TreasuryBlockGenerator::Proceed, this, i);

	for (uint32_t i = 0; i < m_vThreads.size(); i++)
		m_vThreads[i].join();

	// at least 1 kernel
	{
		Coin dummy; // not a coin actually
		dummy.m_key_type = KeyType::Kernel;
		dummy.m_status = Coin::Unconfirmed;

		ECC::Scalar::Native k = m_pKeyChain->calcKey(dummy);

		TxKernel::Ptr pKrn(new TxKernel);
		pKrn->m_Excess = ECC::Point::Native(Context::get().G * k);

		Merkle::Hash hv;
		pKrn->get_HashForSigning(hv);
		pKrn->m_Signature.Sign(hv, k);

		m_Block.m_vKernelsOutput.push_back(std::move(pKrn));
		m_Offset += k;
	}

	m_Offset = -m_Offset;
	m_Block.m_Offset = m_Offset;

	m_Block.Sort();
	m_Block.DeleteIntermediateOutputs();

	SerializerFile ser;
	ser.m_File.open(m_sPath, std::ofstream::out | std::ofstream::trunc | std::ofstream::binary);
	ser & m_Block;
	ser.m_File.flush();

	bool bbb = m_Block.IsValid(1, 1);

	LOG_INFO() << "Done";

	return 0;
}

void TreasuryBlockGenerator::Proceed(uint32_t i)
{
	ECC::Scalar::Native offset(ECC::Zero);

	Block::Body subBlock;
	ZeroObject(subBlock.m_Subsidy);

	for ( ; i < m_Coins.size(); i += m_vThreads.size())
	{
		const Coin& coin = m_Coins[i];

		Output::Ptr pOutp(new Output);
		pOutp->m_Incubation = m_vIncubationAndKeys[i].first;

		const ECC::Scalar::Native& k = m_vIncubationAndKeys[i].second;

		pOutp->Create(k, coin.m_amount);

		subBlock.m_vOutputs.push_back(std::move(pOutp));
		offset += k;
		subBlock.m_Subsidy += coin.m_amount;
	}

	std::unique_lock<std::mutex> scope(m_Mutex);

	m_Offset += offset;
	m_Block.m_Subsidy += subBlock.m_Subsidy;
	m_Block.m_vOutputs.reserve(m_Block.m_vOutputs.size() + subBlock.m_vOutputs.size());

	for (i = 0; i < subBlock.m_vOutputs.size(); i++)
		m_Block.m_vOutputs.push_back(std::move(subBlock.m_vOutputs[i]));
}

int main(int argc, char* argv[])
{
    int logLevel = LOG_LEVEL_DEBUG;
#if LOG_VERBOSE_ENABLED
    logLevel = LOG_LEVEL_VERBOSE;
#endif
    auto logger = beam::Logger::create(logLevel, logLevel);

    po::options_description general_options("General options");
    general_options.add_options()
        (cli::HELP_FULL, "list of all options")
        (cli::MODE, po::value<string>()->required(), "mode to execute [node|wallet]")
        (cli::PORT_FULL, po::value<uint16_t>()->default_value(10000), "port to start the server on")
        (cli::DEBUG_FULL, "launch in debug mode")
        (cli::WALLET_SEED, po::value<string>(), "secret key generation seed");

    po::options_description node_options("Node options");
    node_options.add_options()
        (cli::STORAGE, po::value<string>()->default_value("node.db"), "node storage path")
        (cli::MINING_THREADS, po::value<uint32_t>()->default_value(0), "number of mining threads(there is no mining if 0)")
		(cli::VERIFICATION_THREADS, po::value<uint32_t>()->default_value(0), "number of threads for cryptographic verifications (0 = single thread)")
		(cli::MINER_ID, po::value<uint32_t>()->default_value(0), "seed for miner nonce generation")
		(cli::NODE_PEER, po::value<std::vector<std::string> >(), "nodes to connect to")
		//(cli::TREASURY_BLOCK, po::value<string>(), "Treasury block to generate genesis block from")
		;

    po::options_description wallet_options("Wallet options");
    wallet_options.add_options()
        (cli::PASS, po::value<string>()->default_value(""), "password for the wallet")
        (cli::AMOUNT_FULL, po::value<ECC::Amount>(), "amount to send")
        (cli::RECEIVER_ADDR_FULL, po::value<string>(), "address of receiver")
        (cli::NODE_ADDR_FULL, po::value<string>(), "address of node")
		(cli::TREASURY_BLOCK, po::value<string>(), "Block to create/append treasury to")
		(cli::COMMAND, po::value<string>(), "command to execute [send|listen|init|info|treasury]");

    po::options_description options{ "Allowed options" };
    options.add(general_options)
           .add(node_options)
           .add(wallet_options);

    po::positional_options_description pos;
    pos.add(cli::MODE, 1);

    try
    {
        po::variables_map vm;

		{
			std::ifstream cfg("beam.cfg");

			if (cfg)
			{
				po::store(po::parse_config_file(cfg, options), vm);
			}
		}

        po::store(po::command_line_parser(argc, argv)
            .options(options)
            .positional(pos)
            .run(), vm);

        if (vm.count(cli::HELP))
        {
            printHelp(options);

            return 0;
        }

        po::notify(vm);

        auto port = vm[cli::PORT].as<uint16_t>();
        auto debug = vm.count(cli::DEBUG) > 0;
        auto hasWalletSeed = vm.count(cli::WALLET_SEED) > 0;

        if (vm.count(cli::MODE))
        {
            io::Reactor::Ptr reactor(io::Reactor::create());
            io::Reactor::Scope scope(*reactor);
            NoLeak<uintBig> walletSeed;
            walletSeed.V = Zero;
            if (hasWalletSeed)
            {
                auto seed = vm[cli::WALLET_SEED].as<string>();
                Hash::Value hv;
                Hash::Processor() << seed.c_str() >> hv;
                walletSeed.V = hv;
            }

            auto mode = vm[cli::MODE].as<string>();
            if (mode == cli::NODE)
            {
                beam::Node node;

                node.m_Cfg.m_Listen.port(port);
                node.m_Cfg.m_Listen.ip(INADDR_ANY);
                node.m_Cfg.m_sPathLocal = vm[cli::STORAGE].as<std::string>();
                node.m_Cfg.m_MiningThreads = vm[cli::MINING_THREADS].as<uint32_t>();
                node.m_Cfg.m_MinerID = vm[cli::MINER_ID].as<uint32_t>();
				node.m_Cfg.m_VerificationThreads = vm[cli::VERIFICATION_THREADS].as<uint32_t>();
				node.m_Cfg.m_TestMode.m_bFakePoW = debug;
                if (node.m_Cfg.m_MiningThreads > 0 && !hasWalletSeed)
                {
                    LOG_ERROR() << " wallet seed is not provided. You have pass wallet seed for mining node.";
                    return -1;
                }
                node.m_Cfg.m_WalletKey = walletSeed;

				std::vector<std::string> vPeers;

				if (vm.count(cli::NODE_PEER))
					vPeers = vm[cli::NODE_PEER].as<std::vector<std::string> >();

				node.m_Cfg.m_Connect.resize(vPeers.size());

				for (size_t i = 0; i < vPeers.size(); i++)
				{
					io::Address& addr = node.m_Cfg.m_Connect[i];
					if (!addr.resolve(vPeers[i].c_str()))
					{
						LOG_ERROR() << "unable to resolve: " << vPeers[i];
						return -1;
					}

					if (!addr.port())
					{
						if (!port)
						{
							LOG_ERROR() << "Port must be specified";
							return -1;
						}
						addr.port(port);
					}
				}

                LOG_INFO() << "starting a node on " << node.m_Cfg.m_Listen.port() << " port...";

                node.Initialize();

				std::string sPath = vm[cli::TREASURY_BLOCK].as<string>();
				if (!sPath.empty())
				{
					Block::Body block;

					{
						std::ifstream f(sPath, std::ifstream::binary);
						if (f.fail())
						{
							LOG_ERROR() << "can't open treasury file";
							return -1;
						}

						std::vector<char> vContents((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());

						Deserializer der;
						der.reset(&vContents.at(0), vContents.size());

						der & block;
					}

					node.GenerateGenesisBlock(block);
				}

                reactor->run();
            }
            else if (mode == cli::WALLET)
            {
                if (vm.count(cli::COMMAND))
                {
                    auto command = vm[cli::COMMAND].as<string>();
                    if (command != cli::INIT
                     && command != cli::SEND
                     && command != cli::LISTEN
                     && command != cli::TREASURY
                     && command != cli::INFO)
                    {
                        LOG_ERROR() << "unknown command: \'" << command << "\'";
                        return -1;
                    }

                    LOG_INFO() << "starting a wallet...";

                    std::string pass(vm[cli::PASS].as<std::string>());
                    if (!pass.size())
                    {
                        LOG_ERROR() << "Please, provide password for the wallet.";
                        return -1;
                    }

                    if (command == cli::INIT)
                    {
                        if (!hasWalletSeed)
                        {
                            LOG_ERROR() << "Please, provide seed phrase for the wallet.";
                            return -1;
                        }
                        auto keychain = Keychain::init(pass, walletSeed);
                        if (keychain)
                        {
                            LOG_INFO() << "wallet successfully created...";
                            if (debug)
                            {
                                for(auto amount : {5, 10, 20, 50, 100, 200, 500})
                                {
                                    Coin coin(amount);
                                    keychain->store(coin);
                                }

                                LOG_INFO() << "wallet with coins successfully created...";
                            }
                            return 0;
                        }
                        else
                        {
                            LOG_ERROR() << "something went wrong, wallet not created...";
                            return -1;
                        }
                    }

                    auto keychain = Keychain::open(pass);
                    if (!keychain)
                    {
                        LOG_ERROR() << "something went wrong, wallet not opened...";
                        return -1;
                    }

                    LOG_INFO() << "wallet sucessfully opened...";

					if (command == cli::TREASURY)
					{
						TreasuryBlockGenerator tbg;
						tbg.m_sPath = vm[cli::TREASURY_BLOCK].as<std::string>();
						tbg.m_pKeyChain = keychain.get();

						// TODO: command-line parameter
						Height dh = 60 * 2; // 2 hours, 12 per day
						uint32_t nCount = 12 * 30; // 360 total. 1 month roughly

						return tbg.Generate(nCount, dh);
					}

                    if (command == cli::INFO)
                    {
                        cout << "____Wallet summary____\n\n";
                        cout << "| id\t| amount\t| height\t| status\t| key type\t|\n";
                        keychain->visit([](const Coin& c)->bool
                        {
                            cout << setw(8) << c.m_id
                                 << setw(16) << c.m_amount
                                 << setw(16) << c.m_height
                                 << "  " << c.m_status << '\t'
                                 << "  " << c.m_key_type << '\n';
                            return true;
                        });
                        return 0;
                    }

                    // resolve address after network io
                    io::Address node_addr;
                    node_addr.resolve(vm[cli::NODE_ADDR].as<string>().c_str());
                    bool is_server = command == cli::LISTEN;
                    WalletNetworkIO wallet_io{ io::Address().ip(INADDR_ANY).port(port)
                                             , node_addr
                                             , is_server
                                             , keychain
                                             , reactor};

					wallet_io.sync_with_node([&]()
					{
						if (command == cli::SEND)
						{
							auto amount = vm[cli::AMOUNT].as<ECC::Amount>();
							io::Address receiver_addr;
							receiver_addr.resolve(vm[cli::RECEIVER_ADDR].as<string>().c_str());

							LOG_INFO() << "sending money " << receiver_addr.str();
							wallet_io.transfer_money(receiver_addr, move(amount));
						}
					});

                    wallet_io.start();
                }
                else
                {
                    LOG_ERROR() << "command parameter not specified.";
                    printHelp(options);
                }
            }
            else
            {
                LOG_ERROR() << "unknown mode \'" << mode << "\'.";
                printHelp(options);
            }
        }
    }
    catch(const po::error& e)
    {
        LOG_ERROR() << e.what();
        printHelp(options);
    }

    return 0;
}

cmake_minimum_required(VERSION 3.13)

add_executable(laser_beam_demo laser_beam_demo.cpp)
target_link_libraries(laser_beam_demo node)

add_executable(node_net_sim node_net_sim.cpp)
target_link_libraries(node_net_sim node mnemonic cli)

if(LINUX)
	target_link_libraries(laser_beam_demo -static-libstdc++ -static-libgcc)
	target_link_libraries(node_net_sim -static-libstdc++ -static-libgcc)
endif()

target_link_libraries(node_net_sim Boost::program_options)

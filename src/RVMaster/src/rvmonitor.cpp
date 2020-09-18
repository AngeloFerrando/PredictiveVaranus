#include "rvmonitor.h"

using namespace std;
namespace rv
{
    // Declarations of shared variables
    

    namespace monitor
    {
        std::set<std::string> monitorTopics;
        std::set<std::string> allMonitors;
        std::set<std::string> enabledMonitors;
        std::map<std::string,std::string> topicsAndTypes;

        void initMonitorTopics()
        {
            monitorTopics.insert("/turtle1/command_velocity");
            topicsAndTypes["/turtle1/command_velocity"] = "turtlesim/Velocity";
            monitorTopics.insert("/turtle1/pose");
            topicsAndTypes["/turtle1/pose"] = "turtlesim/Pose";

            allMonitors.insert("turtlesimLog");

        }

        void initAdvertiseOptions(std::string topic, ros::AdvertiseOptions &ops_pub)
        {
            if (topic == "/turtle1/command_velocity") {
                ops_pub.init<turtlesim::Velocity>(topic, 1000);
            }
            else if (topic == "/turtle1/pose") {
                ops_pub.init<turtlesim::Pose>(topic, 1000);
            }
        }

    }

    RVMonitor::RVMonitor(string topic, ros::SubscribeOptions &ops_sub)
    {
        topic_name = topic;
        server_manager = rv::ServerManager::instance();

        if (topic == "/turtle1/command_velocity") {
            ops_sub.init<turtlesim::Velocity>(topic, 1000, boost::bind(&RVMonitor::monitorCallback_turtleVelocity, this, _1));
        }
        else if (topic == "/turtle1/pose") {
            ops_sub.init<turtlesim::Pose>(topic, 1000, boost::bind(&RVMonitor::monitorCallback_turtlePose, this, _1));
        }
    }

    void RVMonitor::monitorCallback_turtleVelocity(const turtlesim::Velocity::ConstPtr& monitored_msg)
    {

        turtlesim::Velocity rv_msg;


        float& L = rv_msg.linear;
        float& A = rv_msg.angular;

        if(monitor::enabledMonitors.find("turtlesimLog") != monitor::enabledMonitors.end())
        {
		ROS_INFO("turtlesim/Velocity linear: %f, angular: %f", L, A);
	}


        ros::SerializedMessage serializedMsg = ros::serialization::serializeMessage(rv_msg);
        server_manager->publish(topic_name, serializedMsg);
    }

    void RVMonitor::monitorCallback_turtlePose(const turtlesim::Pose::ConstPtr& monitored_msg)
    {

        turtlesim::Pose rv_msg;
        rv_msg.linear_velocity = monitored_msg->linear_velocity;
        rv_msg.x = monitored_msg->x;
        rv_msg.y = monitored_msg->y;
        rv_msg.theta = monitored_msg->theta;
        rv_msg.angular_velocity = monitored_msg->angular_velocity;


        float& X = rv_msg.x;
        float& Y = rv_msg.y;
        float& Th = rv_msg.theta;

        if(monitor::enabledMonitors.find("turtlesimLog") != monitor::enabledMonitors.end())
        {
		ROS_INFO("turtlesim/Pose x: %f, y: %f, theta: %f", X, Y, Th);
	}


        ros::SerializedMessage serializedMsg = ros::serialization::serializeMessage(rv_msg);
        server_manager->publish(topic_name, serializedMsg);
    }


}


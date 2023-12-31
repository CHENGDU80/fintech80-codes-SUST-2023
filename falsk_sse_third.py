# encoding=utf-8
from flask import Flask, stream_with_context, Response, request
from flask_cors import CORS
import requests
import json

# from time import sleep

content1 = "请你分析这篇新闻：{}"
# 股票
messages1 = [{"role": "user",
              "content": "现在你是一个金融分析师，需要分析新闻对股票投资的影响，请你先不要做出回答，看看以下这篇示例回答，你在回答新闻的影响时需要按照它的逻辑框架以及严格按照它的格式（一定严格按照它的格式来！）。 示例回答：[**总体来看这篇新闻中更偏向于投资机会。**以下是详细分析部分： **投资机会：** 1. **产能恢复**：公司预计在11月份将产能恢复到历时最高水平。这显示了公司对市场的乐观态度，认为市场需求将继续增长，可能为公司带来更多订单和收入。 2. **产品价格回调**：公司已经完成了今年的涨价，并且回调了所有客户的价格，表明公司在市场上具有一定的定价权，有能力保持合理的利润水平。 3. **新项目价格回归**：新项目的价格已经回归到合理水平，这显示了公司对业务拓展和新项目的良好掌控能力。 4. **2024年市场预测**：预计到2024年，市场上中小尺寸液晶显示模组产品价格将处于合理水平，这为公司未来的稳定发展提供了积极信号。 **风险：** 1. **市场不确定性**：尽管公司对市场回暖持乐观态度，但市场状况可能会受到外部因素的影响，如全球经济波动、政治动荡等。 2. **竞争压力**：液晶显示模组行业竞争激烈，公司需要保持技术领先地位，提升产品质量和性能，以保持市场竞争力。] 3. **供应链风险**：全球供应链问题可能会对公司的生产和交付产生不利影响，例如原材料短缺、运输问题等。 4. **外部经济因素**：宏观经济因素如通货膨胀、汇率波动等可能会对公司的盈利能力产生影响。 **总结：** 虽然存在一些风险，但从目前的情况来看，公司似乎处于一个积极的发展阶段，市场回暖和公司的策略调整为投资者提供了一些机会。然而，投资者应该保持谨慎态度，密切关注市场动态和公司业绩，以做出明智的投资决策"},
             {"role": "assistant",
              "content": "好的我知道了，并且我会直接给出回答，不会在开头说与影响分析无关的话，也不会在结尾总结"},
             {"role": "user",
              "content": ""}
             ]

# 行业
messages2 = [{"role": "user",
              "content": "现在你是一个金融分析师，需要分析新闻对行业投资的影响，请你先不要做出回答，看看以下这篇示例回答，你在回答新闻的影响时需要严格按照它的逻辑框架以及严格按照它的格式（一定严格按照它的格式来！）。 示例回答：投资机会：\n1. **基建投资机会**：政府推动的“1万亿元国债”将为水泥行业提供了一个发展契机，预计会带动基建项目的增加，从而拉动水泥需求，为行业发展提供了有利条件。\n2. **海外市场拓展**：部分水泥企业已经开始将业务扩展至“一带一路”沿线国家和地区，这一战略可以带来新的市场和业务增长点，降低对国内市场的依赖度，有利于企业长期稳健发展。\n投资风险：\n1. **成本压力**：新闻中提到的成本压力可能会影响企业的盈利能力，如果原材料价格持续上涨，或者生产成本增加，可能对企业的利润产生负面影响。\n2. **需求不足**：尽管水泥企业发出涨价通知，但如果需求端的动力不足，市场可能无法承受涨价带来的价格压力，从而导致涨价难以落实，影响企业盈利能力。\n3. **环保政策压力**：水泥行业作为高能耗、高排放行业，受到环保政策的严格监管，企业需要投入大量资源用于环保设施建设和运营，未能有效应对环保要求可能会面临处罚或者停产整顿等风险。\n4. **国际市场不确定性**：拓展至海外市场可能会面临当地政策、法规、文化等方面的挑战，以及国际市场波动可能带来的业务风险。\n5. **库存压力**：新闻中提到的错峰生产库存压力减缓，可能意味着部分企业目前还面临库存积压的情况，这可能会对企业的资金流动和经营状况产生一定影响。\n总的来说，水泥行业存在基建投资和海外市场拓展的机会，但同时也面临成本压力、需求不足、环保政策、国际市场不确定性以及库存压力等风险。投资者在考虑投资水泥行业时，需要综合考虑行业发展趋势、企业自身情况以及风险因素，并制定相应的风险管理策略。同时，也建议在投资前进行充分的市场调研和尽职调查"},
             {"role": "assistant",
              "content": "好的我知道了，并且我会直接给出回答，不会在开头说与影响分析无关的话，也不会在结尾总结"},
             {"role": "user",
              "content": ""}
             ]

# 期货
messages3 = [{"role": "user",
              "content": "现在你是一个金融分析师，需要分析新闻对商品期货投资的影响，请你先不要做出回答，看看以下这边篇回答，你在回答新闻的影响时需要按照它的逻辑框架以及它的格式（请一定严格按照它的格式来！）：在回答的最开始，需要将三个领域重要性做个排序，例如：**能源化工 > 金属矿产 > 农产品** **能源化工：** - **最高影响**: 能源化工将受到最显著影响，因其与地缘政治紧张局势和油价相关。冲突可能会扰乱石油供应链，导致价格飙升。投资者应密切关注全球能源市场的动向。 - **原因**: 地缘政治冲突通常会导致能源市场的波动加剧，影响石油及相关产品的价格。此外，供应链中断可能影响能源化工的生产和分销。 **金属矿产：** - **中等影响**: 金属和矿产将受到适度影响。虽然冲突可能会扰乱采矿业务，但影响可能不如能源市场那样直接。投资者应关注具有区域依赖性的特定金属。 - **原因**: 采矿部门可能会在受到冲突影响的地区经历生产中断，可能导致供应短缺。然而，与能源市场相比，冲击的直接影响可能会较小。 **农产品：** - **最低影响**: 农产品受到的冲击可能最小。虽然冲突可能会导致农业活动在受影响地区受阻，但全球农业市场通常对地区冲突具有一定的抵御能力。在这个领域的投资者应该密切关注局势发展，但可以预期相对稳定的市场状况。 - **原因**: 农业市场主要受到天气模式、全球需求和贸易政策等因素的影响。虽然冲突可能会导致局部扰乱，但总体上对农业市场的影响通常较能源和金属市场为小。 总的来说，能源化工受冲突影响最大，因其与地缘政治紧张局势和油价相关。金属矿产其次，由于采矿业务的中断可能会影响供应链。农产品受影响最小，因其市场主要受天气、全球需求和贸易政策等因素影响。然而，所有投资者都应密切关注局势发展，并随时准备调整他们的策略"},
             {"role": "assistant",
              "content": "好的我知道了，并且我会直接给出回答，不会在开头说与影响分析无关的话，也不会在结尾总结"},
             {"role": "user",
              "content": ""}
             ]

content4 = "现在你是金融分析师，请你对中国A股{}行业进行投资分析，内容需要包括行业近况，投资机会，投资风险，推荐股票，字数在150字左右。请按照示例回答的逻辑框架及格式来，示例回答：中国A股半导体行业近况显示出强劲的增长势头。随着数字化技术的普及和5G等新兴技术的发展，半导体市场需求持续增加，行业呈现出良好的发展态势。\n投资机会方面，中国政府加大了对半导体产业的支持力度，鼓励技术创新和产业升级，为企业提供了良好的政策环境。此外，随着全球产业链的调整，国内半导体企业有望在一些细分领域实现突破，比如物联网、人工智能等。\n然而，投资者也需要注意一些风险因素。首先，半导体产业的技术周期较长，市场竞争激烈，新兴技术的快速发展也带来了市场不确定性。其次，国际贸易紧张局势可能会对行业产生一定影响。\n针对投资推荐，建议关注具备核心技术、研发实力雄厚的领军企业，如中芯国际（688981）和华星光电（688819）等，但投资前需谨慎评估行业和企业的具体情况，做好风险控制措施。"
# 行业分析接口
messages4 = [{"role": "user",
              "content": ""
              }]

content5 = "现在你是金融分析师，请你对大宗商品{}进行投资分析，内容需要包括商品简介，行业近况，投资机会，投资风险，字数在150字左右。请按照示例回答的逻辑框架及格式来，示例回答：大豆是一种重要的大宗商品，其主要用途包括食品、饲料和工业原料。近年来，全球大豆产量稳步增长，但受气候变化和贸易摩擦等因素影响，价格波动较大。当前，大豆市场面临供需紧张和国际政治经济环境不确定性等压力，但同时也存在投资机会。\n投资机会方面，随着人们对健康食品的需求增加，大豆制品的消费逐渐增长。此外，动物饲料行业也是大豆需求的主要来源，随着全球肉类消费的持续增加，对大豆的需求也将保持稳定。\n然而，投资大豆也伴随着一定的风险。首先，全球气候变化可能会影响大豆产量，从而导致价格波动。其次，贸易政策和国际政治紧张局势可能导致大豆价格的不确定性。\n总的来说，投资大豆需要综合考虑供需情况、气候因素和国际政治经济环境等多方面因素，谨慎评估风险和机会，采取相应的投资策略。"
# 大宗期货
messages5 = [{"role": "user",
              "content": ""},
             ]

content6 = "现在你是金融分析师，请你对中国A股{}进行投资分析，内容需要包括公司近况，投资机会，投资风险，字数在150字左右。请直接开始分析，开头不要说废话"
# 股票接口
messages6 = [{"role": "user",
              "content": ""},
             ]

content7 = "{}请为这段新闻写一句摘要，摘要中既要概括新闻内容，也要有一句话总结该新闻背后的市场投资机会或风险（如果该条新闻是宏观新闻或者政治新闻，则分析商品期货市场的投资风险或机会，如果是股票或行业相关新闻，则分析股票市场的投资风险或机会），字数控制在50字以内"
# 摘要接口
messages7 = [{"role": "user",
              "content": ""},
             ]

app = Flask(__name__)
CORS(app)  # 这样做是为了跨域支持，因为前端可能在不同的域上运行

API_URL = "https://flag.smarttrot.com/v1/chat/completions"
HEADERS = {
    "Authorization": "Bearer zk-b9fa49f26222ea6edc89d0ccd69ff7af",
    "Content-Type": "application/json"
}


@app.route('/sse/')
def sse():
    type = request.args.get('type')
    content = request.args.get('content')
    index = request.args.get('index')
    # print(type, content, index)
    # print(content1.format(content))
    if type == 'summary':  # 新闻摘要
        c = content7.format(content)
        messages7[0]['content'] = c
        messages = messages7
    elif type == 'news':  # 新闻分析
        c = content1.format(content)
        if index == 1:
            messages1[3]['content'] = c
            messages = messages1
        elif index == 2:
            messages2[3]['content'] = c
            messages = messages2
        else:
            messages3[3]['content'] = c
            messages = messages3
    elif type == 'medicine' or type == 'semi' or type == 'bank':  # 行业
        c = content4.format(content)
        messages4[0]['content'] = c
        messages = messages4
    elif type == 'qihuo':  # 期货
        c = content5.format(content)
        messages5[0]['content'] = c
        messages = messages5
    else:  # 股票
        c = content6.format(content)
        messages6[0]['content'] = c
        messages = messages6

    payload = {
        "model": "gpt-4-0314",
        "messages": messages,
        "stream": True,
        "temperature": 0.2
    }

    def generate():
        with requests.post(API_URL, headers=HEADERS, json=payload, stream=True) as response:
            # print(response.text)
            for line in response.iter_lines():
                line = line.decode('utf-8').replace("data: ", "")

                if line and line != '[DONE]':
                    # print(line)
                    data = json.loads(line)
                    # print(data)
                    if "choices" in data:
                        content = data["choices"][0].get("delta", {}).get("content")
                        if content is not None:
                            yield f"data: {content}\n\n"
                #         # sleep(0.5)

    return Response(generate(), content_type="text/event-stream")


if __name__ == '__main__':
    app.run(debug=True)


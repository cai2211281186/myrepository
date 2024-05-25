from sqlalchemy import create_engine, Column, Integer, String, TEXT
from sqlalchemy.orm import sessionmaker, declarative_base, scoped_session

# 创建数据库引擎
engine = create_engine('mysql+pymysql://root:123456@localhost/fileKnowledge')

Base = declarative_base()


class data(Base):
    __tablename__ = 'data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(255))
    score = Column(String(255))
    director = Column(String(255))
    Scriptwriter = Column(String(255))
    star = Column(TEXT)
    types = Column(String(255))
    making = Column(String(255))
    language = Column(String(255))
    Release_time = Column(String(255))
    timing = Column(String(255))
    introduction = Column(TEXT)


def chooseData(temp_list):
    seen = set()
    flattened_unique_list = [
        subitem for item in temp_list
        for subitem in item.split('/') if subitem and subitem not in seen and not seen.add(subitem)
    ]
    return flattened_unique_list


# 创建会话工厂
Session = scoped_session(sessionmaker(bind=engine))


def get_data_by_column_sync(column_name, split_by_slash=False):
    if hasattr(data, column_name):
        # 使用上下文管理器自动管理会话（同步）
        with Session() as session:
            column_attr = getattr(data, column_name)
            datas = session.query(column_attr).distinct().all()

            if split_by_slash:
                # 对于需要切分的列，先切分再去重
                flattened_data_set = set()
                for data_obj in datas:
                    # 检查值是否为None或空字符串
                    value = data_obj[0]
                    if value is not None and value.strip():
                        subitems = value.split('/')
                        flattened_data_set.update(subitem for subitem in subitems if subitem.strip())
                return list(flattened_data_set)  # 转换为列表并返回
            else:
                # 对于其他列，直接返回去重后的值列表（尽管这里已经通过distinct查询去重了）
                return [getattr(data_obj[0], column_name) for data_obj in datas if
                        getattr(data_obj[0], column_name) is not None]
    else:
        raise ValueError(f"Invalid column name: {column_name}")

    # 使用示例


if __name__ == '__main__':
    print(get_data_by_column_sync('star', True))

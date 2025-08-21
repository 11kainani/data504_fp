class Talent:
    def __init__(self, talent_dict) -> None:
        self.name = talent_dict['name'] or ''
        self.date = talent_dict['date'] or ''
        self.tech_self_score = talent_dict['tech_self_score'] or ''
        self.strengths = talent_dict['strengths'] or ''
        self.weaknesses = talent_dict['weaknesses'] or ''
        self.self_development = talent_dict['self_development'] or ''
        self.geo_flex = talent_dict['geo_flex'] or ''
        self.financial_support_self = talent_dict['financial_support_self'] or ''
        self.result = talent_dict['result'] or ''
        self.course_interest = talent_dict['course_interest'] or ''

    def get_info(self):
        return {'name': self.name, 'date': self.date, 'tech_self_score': self.tech_self_score, 'strengths': self.strengths}
